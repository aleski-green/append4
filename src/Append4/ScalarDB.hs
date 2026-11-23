-- A B-tree of complete keys to scalar values. Overflow indexes only accommodate
-- LMDB's key length limit; they contain no document roots or field paths.
module Append4.ScalarDB where
import qualified Append4.LMDB as L
import qualified Append4.Flat as F
import qualified Data.ByteString as B
import qualified Data.Map.Strict as M
import qualified Crypto.Hash.SHA256 as SHA
import qualified Data.Text.Encoding as T
import Control.Monad

data DB = DB L.DB L.DB L.DB
open :: L.Txn -> String -> IO DB
open tx name = DB <$> L.openDB tx name <*> L.openDB tx (name++".long-values") <*> L.openDB tx (name++".long-keys")
existing :: L.Txn -> String -> IO DB
existing tx name = DB <$> L.existingDB tx name <*> L.existingDB tx (name++".long-values") <*> L.existingDB tx (name++".long-keys")
long :: B.ByteString -> Bool
long key = B.length key>500
get :: L.Txn -> DB -> B.ByteString -> IO (Maybe F.Atom)
get tx (DB base vals keys) key = if not(long key) then L.get tx base key >>= traverse F.decode else do
 let h=SHA.hash key
 found <- L.get tx keys h
 case found of
  Nothing -> pure Nothing
  Just bytes -> do
   name <- F.decode bytes
   unless (name==F.TextAtom (T.decodeUtf8 key)) (fail "Full-key hash collision")
   L.get tx vals h >>= traverse F.decode
put :: L.Txn -> DB -> B.ByteString -> F.Atom -> IO ()
put tx (DB base vals keys) key value = if not(long key) then L.put tx base key (F.encode value) else do
 let h=SHA.hash key; name=F.TextAtom (T.decodeUtf8 key)
 found <- L.get tx keys h
 forM_ found $ \bytes -> F.decode bytes >>= \old -> unless(old==name)(fail "Full-key hash collision")
 L.put tx keys h (F.encode name)
 L.put tx vals h (F.encode value)
delete :: L.Txn -> DB -> B.ByteString -> IO ()
delete tx db@(DB base vals keys) key = if not(long key) then L.delete tx base key else do
 void(get tx db key)
 L.delete tx vals (SHA.hash key)
 L.delete tx keys (SHA.hash key)
scan :: L.Txn -> DB -> B.ByteString -> Maybe B.ByteString -> Maybe B.ByteString -> Int -> IO [(B.ByteString,F.Atom)]
scan tx db@(DB base _ keys) lower upper prefix limit = do
 direct <- L.scan tx base lower upper prefix limit >>= mapM (\(k,v)->(,) k <$> F.decode v)
 indexed <- L.scan tx keys "" Nothing Nothing maxBound
 extra <- fmap concat $ forM indexed $ \(_,v)->do
  name <- F.decode v
  key <- case name of F.TextAtom t -> pure(T.encodeUtf8 t); _ -> fail "Invalid complete-key index"
  if key>=lower && maybe True (key<) upper && maybe True (`B.isPrefixOf` key) prefix
   then get tx db key >>= maybe (fail "Missing long-key value") (\a->pure[(key,a)]) else pure []
 pure(take limit(M.toAscList(M.fromList(direct++extra))))
