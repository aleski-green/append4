-- API serializers only: no root/index/document records are persisted.
module Append4.Key where
import qualified Append4.Flat as F
import Data.Aeson
import qualified Data.Aeson.KeyMap as KM
import qualified Data.Aeson.Key as K
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import Data.Text(Text)
import qualified Data.Vector as V
import Control.Monad

escape :: Text -> Text
escape "" = "~e"
escape t = T.replace "/" "~1" (T.replace "~" "~0" t)
unescape :: Text -> Text
unescape "~e" = ""
unescape t = T.replace "~0" "~" (T.replace "~1" "/" t)
rootKey :: Text -> Text
rootKey = T.intercalate "/" . map escape . T.splitOn "/"
segment :: F.Segment -> Text
segment (F.Field t) = escape t
segment (F.Index n) = "~i"<>T.pack(show n)
fieldKey :: Text -> [F.Segment] -> Text
fieldKey root p = T.intercalate "/" (rootKey root:map segment p)
flatten :: Text -> Value -> M.Map Text F.Atom
flatten root value = M.fromList [(fieldKey root p,a) | (p,a)<-M.toList(F.flatten value),keep p a]
 where
 keep p F.ObjectAtom = case valueAt p value of Object o -> KM.null o; _ -> False
 keep _ (F.ArrayAtom n) = n==0
 keep _ _ = True
 valueAt [] v=v
 valueAt (F.Field t:ps) (Object o)=maybe Null (valueAt ps) (KM.lookup(K.fromText t)o)
 valueAt (F.Index i:ps) (Array a)=maybe Null(valueAt ps)(a V.!? fromIntegral i)
 valueAt _ _=Null

unflatten :: Text -> M.Map Text F.Atom -> IO (Maybe Value)
unflatten root allCells = do
 let key=rootKey root; prefix=key<>"/"
     selected=M.union (maybe M.empty (M.singleton key) (M.lookup key allCells))
       (M.takeWhileAntitone (<key<>"0") (M.dropWhileAntitone (<prefix) allCells))
 if M.null selected then pure Nothing else do
  rows<-forM(M.toList selected)$ \(k,a)->do
   p<-if k==key then pure [] else mapM parse(T.splitOn "/" (T.drop(T.length prefix)k))
   pure(p,a)
  let leaves=M.fromList rows
      parents=M.fromListWith (++) [(take n p,[p!!n]) | (p,_)<-rows,n<-[0..length p-1]]
  containers<-forM(M.toList parents)$ \(p,children)->do
   when(M.member p leaves)(fail "Scalar key conflicts with descendant keys")
   let unique=M.keys(M.fromList[(s,())|s<-children])
   a<-case unique of
    F.Field _:_->if all isField unique then pure F.ObjectAtom else fail "Mixed array and field segments"
    F.Index _:_->do
     let indices=[i|F.Index i<-unique]
     unless (length indices==length unique && indices==[0..fromIntegral(length indices)-1]) (fail "Sparse array keys")
     pure(F.ArrayAtom(fromIntegral(length indices)))
    _->fail "Invalid key hierarchy"
   pure(p,a)
  Just <$> F.unflatten(M.union leaves(M.fromList containers))
 where
 isField(F.Field _)=True
 isField _=False
 parse s | Just number<-T.stripPrefix "~i" s = case reads(T.unpack number) of
            [(n,"")]|n>=0 && "~i"<>T.pack(show n)==s->pure(F.Index n)
            _->fail "Invalid array index"
         | otherwise = pure(F.Field(unescape s))
