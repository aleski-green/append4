-- Explicit offline v2 -> v3 conversion. The application owns publication.
module Append4.Convert (convert, readPriorHistory) where
import qualified Append4 as Old
import qualified Append4.KV as N
import qualified Append4.Flat as F
import qualified Append4.Key as Key
import qualified Append4.LMDB as L
import qualified Append4.ScalarDB as S
import Control.Exception
import Control.Concurrent (runInBoundThread)
import Control.Monad
import Data.Aeson
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.ByteString as B
import System.Directory

parse v = case fromJSON v of Error e->fail e; Success a->pure a
flattenRecords rows = foldM add M.empty rows where
 add cells (k,v) = do
  let more=Key.flatten k v
  unless(M.null(M.intersection cells more))(fail("Colliding complete keys at "++T.unpack k))
  pure(M.union cells more)
convert :: N.Options -> FilePath -> FilePath -> IO ()
convert options source target = do
 exists<-doesFileExist target
 when exists(fail "Single-key migration target already exists")
 size<-getFileSize source
 let capacity=min(N.maximumMapBytes options)(max(N.initialMapBytes options)(size*64))
 bracket(L.openOffline source)L.close $ \old -> bracket(L.begin old True)L.abort $ \input -> do
  let readRows name=F.existing False input name >>= \db -> F.scan input db "" Nothing Nothing maxBound
  roots<-concat <$> mapM readRows ["state","receipts","expiry"]
  let originals=[(T.decodeUtf8 k,v)|(k,v)<-roots]
  expected<-flattenRecords originals
  history<-mapM (parse . snd) =<< readRows "audit" :: IO [Old.Append]
  meta<-readRows "metadata"
  bracket(L.open target capacity)L.close $ \new -> runInBoundThread $ do
   L.write new $ \output -> do
    dbs<-N.Databases <$> S.open output "state" <*> S.open output "receipts" <*> S.open output "expiry" <*> S.open output "audit" <*> S.open output "metadata"
    archive<-S.open output "prior-history"
    let dbFor k | "receipts/" `T.isPrefixOf` k=N.receiptsDB dbs
                | "expiry/" `T.isPrefixOf` k=N.expiryDB dbs
                | otherwise=N.stateDB dbs
        putCells db rows=forM_(M.toList rows)$ \(k,a)->S.put output db (T.encodeUtf8 k) a
    forM_(M.toList expected)$ \(k,a)->S.put output (dbFor k)(T.encodeUtf8 k)a
    forM_ meta $ \(key,v)->unless(key=="format")$case M.toList(F.flatten v) of
     [([],atom)]->S.put output(N.metaDB dbs)key atom
     _->putCells(N.metaDB dbs)(Key.flatten(T.decodeUtf8 key)v)
    S.put output (N.metaDB dbs) "format" (F.TextAtom("append4-single-3:"<>T.pack(show(N.historyPolicy options))))
    let applyRecord records op = case op of Old.Set k v->M.insert k v records;Old.Delete k->M.delete k records
        expand (records,ops) op = do
         let key=case op of Old.Set k _->k;Old.Delete k->k
             previous=maybe M.empty(Key.flatten key)(M.lookup key records)
             next=case op of Old.Set _ v->Key.flatten key v;Old.Delete _->M.empty
             deletes=map N.Delete(M.keys(previous `M.difference` next))
             sets=[N.Set k a|(k,a)<-M.toList next,M.lookup k previous/=Just a]
         pure(applyRecord records op,ops++deletes++sets)
        transaction (records,txs) tx = do
         (next,ops)<-foldM expand (records,[]) (Old.mutations tx)
         let conditions=concat [case v of Nothing->[(Key.rootKey k,Nothing)];Just value->[(p,Just a)|(p,a)<-M.toList(Key.flatten k value)] | (k,v)<-Old.conditions tx]
         pure(next,txs++[N.Transaction(Old.transactionId tx) conditions ops])
        append records item = do
         putCells archive (Key.flatten(N.versionKey(Old.appendId item))(toJSON item))
         (next,txs)<-foldM transaction (records,[]) (Old.transactions item)
         before<-flattenRecords(M.toList records)
         after<-flattenRecords(M.toList next)
         unless(foldl (\s tx->N.apply s(N.mutations tx)) before txs==after)(fail "Historical scalar replay mismatch")
         putCells(N.auditDB dbs)(Key.flatten(N.versionKey(Old.appendId item))(toJSON(N.Append(Old.appendId item)txs)))
         pure next
    replayed<-foldM append M.empty history
    when(N.historyPolicy options==N.KeepHistory)$do
     replayedFlat<-flattenRecords(M.toList replayed)
     unless(replayedFlat==expected)(fail "Original history does not reproduce current state")
   L.sync new
   bracket(L.begin new True)L.abort $ \output -> do
    actual<-fmap M.fromList $ concat <$> forM ["state","receipts","expiry"] (\name->do
      db<-S.existing output name
      rows<-S.scan output db "" Nothing Nothing maxBound
      pure[(T.decodeUtf8 k,a)|(k,a)<-rows])
    unless(actual==expected)(fail "Single-key migration scalar verification failed")
    forM_ originals $ \(key,value)->Key.unflatten key actual >>= \v->unless(v==Just value)(fail "Original value reconstruction failed")
    archive<-S.existing output "prior-history"
    forM_ history $ \item->do
     let root=N.versionKey(Old.appendId item);prefix=T.encodeUtf8(root<>"/")
     cells<-S.scan output archive prefix Nothing(Just prefix)maxBound
     restored<-Key.unflatten root(M.fromList[(T.decodeUtf8 k,a)|(k,a)<-cells])
     unless(restored==Just(toJSON item))(fail "Original history archive verification failed")

readPriorHistory :: FilePath -> IO [Old.Append]
readPriorHistory path = bracket(L.openOffline path)L.close $ \env->bracket(L.begin env True)L.abort $ \tx->do
 db<-S.existing tx "prior-history"
 rows<-S.scan tx db "" Nothing Nothing maxBound
 let cells=M.fromList[(T.decodeUtf8 k,a)|(k,a)<-rows]
     roots=M.keys(M.fromList[(T.takeWhile(/='/') k,())|k<-M.keys cells])
 forM roots $ \root->Key.unflatten root cells >>= maybe(fail "Missing archived history") parse
