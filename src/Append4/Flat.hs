{-# LANGUAGE DeriveGeneric #-}
-- Physical scalar cells. Aeson is only the compatibility view, never the wire
-- encoding. Paths and atoms have explicit, independently versioned binary tags.
module Append4.Flat where
import qualified Append4.LMDB as L
import Control.Monad
import Data.Aeson hiding (encode,decode)
import qualified Data.Aeson as JSON
import qualified Data.Aeson.Key as K
import qualified Data.Aeson.KeyMap as KM
import Data.Binary (Binary(..))
import qualified Data.Binary as Bin
import Data.Binary.Get
import Data.Binary.Put
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Crypto.Hash.SHA256 as SHA
import qualified Data.Map.Strict as M
import Data.Scientific
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import Data.Word

data Segment = Field Text | Index Word64 deriving (Eq,Ord,Show)
data Atom = NullAtom | BoolAtom Bool | TextAtom Text | NumberAtom Integer Int
          | ObjectAtom | ArrayAtom Word64 deriving (Eq,Show)
data DB = Legacy L.DB | Flat L.DB L.DB L.DB

putText :: Text -> Put
putText = put . TE.encodeUtf8
getText :: Get Text
getText = get >>= either (fail . show) pure . TE.decodeUtf8'
instance Binary Segment where
 put (Field t) = putWord8 0 >> putText t
 put (Index i) = putWord8 1 >> putWord64be i
 get = getWord8 >>= \tag -> case tag of
  0 -> Field <$> getText
  1 -> Index <$> getWord64be
  _ -> fail "Invalid path segment"
instance Binary Atom where
 put a = putWord8 1 >> case a of
  NullAtom -> putWord8 0
  BoolAtom b -> putWord8 1 >> put b
  TextAtom t -> putWord8 2 >> putText t
  NumberAtom c e -> putWord8 3 >> put c >> put e
  ObjectAtom -> putWord8 4
  ArrayAtom n -> putWord8 5 >> putWord64be n
 get = do
  version <- getWord8
  unless (version==1) (fail "Unknown flat atom version")
  tag <- getWord8
  case tag of
   0 -> pure NullAtom
   1 -> BoolAtom <$> get
   2 -> TextAtom <$> getText
   3 -> NumberAtom <$> get <*> get
   4 -> pure ObjectAtom
   5 -> ArrayAtom <$> getWord64be
   _ -> fail "Invalid flat atom"
encode :: Binary a => a -> B.ByteString
encode = BL.toStrict . Bin.encode
decode :: Binary a => B.ByteString -> IO a
decode bytes = case Bin.decodeOrFail (BL.fromStrict bytes) of
 Left (_,_,e) -> fail ("Invalid typed storage: "++e)
 Right (rest,_,a) | BL.null rest -> pure a
                  | otherwise -> fail "Trailing bytes in typed storage"

flatten :: Value -> M.Map [Segment] Atom
flatten = M.fromList . walk [] where
 walk p v = case v of
  Null -> [(p,NullAtom)]
  Bool b -> [(p,BoolAtom b)]
  String t -> [(p,TextAtom t)]
  Number n -> [(p,NumberAtom (coefficient n) (base10Exponent n))]
  Object o -> (p,ObjectAtom):concat [walk (p++[Field (K.toText k)]) x | (k,x)<-KM.toList o]
  Array a -> (p,ArrayAtom (fromIntegral (V.length a))):concat [walk (p++[Index (fromIntegral i)]) x | (i,x)<-zip [0::Int ..] (V.toList a)]

unflatten :: M.Map [Segment] Atom -> IO Value
unflatten cells = do
 (value,used) <- walk []
 unless (used==M.size cells) (fail "Orphan typed cells")
 pure value
 where
 children = M.fromListWith (++) [(init p,[last p]) | p<-M.keys cells,not (null p)]
 walk p = case M.lookup p cells of
  Nothing -> fail "Missing typed cell"
  Just a -> case a of
   NullAtom -> pure (Null,1)
   BoolAtom b -> pure (Bool b,1)
   TextAtom t -> pure (String t,1)
   NumberAtom c e -> pure (Number (scientific c e),1)
   ObjectAtom -> do
    xs <- forM (M.findWithDefault [] p children) $ \s -> case s of
     Field t -> do (v,n)<-walk (p++[s]); pure ((K.fromText t,v),n)
     _ -> fail "Invalid object path"
    pure (Object (KM.fromList (map fst xs)),1+sum (map snd xs))
   ArrayAtom count -> do
    let direct = M.findWithDefault [] p children
    unless (toInteger (length direct)==toInteger count) (fail "Invalid array length")
    xs <- mapM (\i -> walk (p++[Index i])) (if count==0 then [] else [0..count-1])
    pure (Array (V.fromList (map fst xs)),1+sum (map snd xs))

open :: Bool -> L.Txn -> String -> IO DB
open old tx name | old = Legacy <$> L.openDB tx name
open _ tx name = Flat <$> L.openDB tx name <*> L.openDB tx (name++".cells") <*> L.openDB tx (name++".paths")
existing :: Bool -> L.Txn -> String -> IO DB
existing old tx name | old = Legacy <$> L.existingDB tx name
existing _ tx name = Flat <$> L.existingDB tx name <*> L.existingDB tx (name++".cells") <*> L.existingDB tx (name++".paths")
roots :: DB -> L.DB
roots (Legacy db) = db
roots (Flat db _ _) = db
legacyDecode :: B.ByteString -> IO Value
legacyDecode = either fail pure . eitherDecodeStrict'

readCells :: L.Txn -> DB -> B.ByteString -> IO (M.Map [Segment] Atom)
readCells _ (Legacy _) _ = fail "Legacy cells need explicit migration"
readCells tx (Flat root cells paths) key = do
 value <- L.get tx root key
 case value of
  Nothing -> pure M.empty
  Just bytes -> do
   a <- decode bytes
   let prefix = SHA.hash key
   entries <- L.scan tx paths prefix Nothing (Just prefix) maxBound
   descendants <- forM entries $ \(physical,encodedPath) -> do
    p <- decode encodedPath
    unless (not (null p) && physical==prefix<>SHA.hash encodedPath) (fail "Invalid typed path index")
    atom <- L.get tx cells physical >>= maybe (fail "Missing indexed scalar") decode
    pure (p,atom)
   pure (M.fromList (([],a):descendants))
getValue :: L.Txn -> DB -> B.ByteString -> IO (Maybe Value)
getValue tx (Legacy db) key = L.get tx db key >>= traverse legacyDecode
getValue tx db key = do
 cells <- readCells tx db key
 if M.null cells then pure Nothing else Just <$> unflatten cells
putValue :: L.Txn -> DB -> B.ByteString -> Value -> IO ()
putValue tx (Legacy db) key v = L.put tx db key (BL.toStrict (JSON.encode v))
putValue tx db@(Flat root cells paths) key v = do
 old <- readCells tx db key
 let new=flatten v; prefix=SHA.hash key
 forM_ (M.keys (old `M.difference` new)) $ \p -> unless (null p) $ do
  let physical=prefix<>SHA.hash (Append4.Flat.encode p)
  L.delete tx cells physical
  L.delete tx paths physical
 forM_ (M.toList new) $ \(p,a) -> when (M.lookup p old/=Just a) $
  if null p then L.put tx root key (Append4.Flat.encode a) else do
   let encodedPath=Append4.Flat.encode p; physical=prefix<>SHA.hash encodedPath
   prior <- L.get tx paths physical
   when (maybe False (/=encodedPath) prior) (fail "Typed path hash collision")
   L.put tx paths physical encodedPath
   L.put tx cells physical (Append4.Flat.encode a)
deleteValue :: L.Txn -> DB -> B.ByteString -> IO ()
deleteValue tx (Legacy db) key = L.delete tx db key
deleteValue tx db@(Flat root cells paths) key = do
 old <- readCells tx db key
 forM_ (M.keys old) $ \p -> unless (null p) $ do
  let physical=SHA.hash key<>SHA.hash (Append4.Flat.encode p)
  L.delete tx cells physical
  L.delete tx paths physical
 L.delete tx root key
scan :: L.Txn -> DB -> B.ByteString -> Maybe B.ByteString -> Maybe B.ByteString -> Int -> IO [(B.ByteString,Value)]
scan tx db lower upper prefix limit = do
 rows <- L.scan tx (roots db) lower upper prefix limit
 forM rows $ \(key,_) -> do
  value <- getValue tx db key >>= maybe (fail "Missing root") pure
  pure (key,value)
