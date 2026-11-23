{-# LANGUAGE CPP #-}
-- Transactional KV over LMDB. Commits are driven by demand, never by a clock.
module Append4.KV where

import qualified Append4.Flat as F
import qualified Append4.ScalarDB as S
import qualified Append4.Key as Key
import qualified Append4.LMDB as DB
import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.Aeson hiding (Options, defaultOptions, Value)
import qualified Data.Aeson as JSON
import Data.Binary.Put (runPut, putWord64be)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import Data.IORef
import Data.List (foldl')
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Word
import GHC.Generics (Generic)
import System.Directory
import System.FileLock
import System.FilePath
#ifndef mingw32_HOST_OS
import qualified System.Posix.IO as Posix
import qualified System.Posix.Unistd as Posix
#endif

type Value = F.Atom

data Mutation = Set Text Value | Delete Text deriving (Eq, Show, Generic)
instance ToJSON Mutation
instance FromJSON Mutation
data Transaction = Transaction { transactionId :: Text, conditions :: [(Text,Maybe Value)], mutations :: [Mutation] }
 deriving (Eq, Show, Generic)
instance ToJSON Transaction
instance FromJSON Transaction
data Outcome = Accepted Integer | NoChange | Conflict | Duplicate | Empty deriving (Eq, Show)
data Append = Append { appendId :: Integer, transactions :: [Transaction] } deriving (Eq, Show, Generic)
instance ToJSON Append
instance FromJSON Append
data Snapshot = Snapshot { version :: Integer, entries :: M.Map Text Value } deriving (Eq, Show)
data HistoryPolicy = KeepHistory | CurrentOnly deriving (Eq, Show)
data Options = Options { historyPolicy :: HistoryPolicy, initialMapBytes :: Integer, maximumMapBytes :: Integer } deriving Show
defaultOptions :: Options
defaultOptions = Options KeepHistory (16*1024*1024) (64*1024*1024*1024)

type Ticket = TMVar (Either SomeException Outcome)
data Command = Write Transaction (Maybe Integer) Ticket | Barrier (TMVar (Either SomeException ())) | Close
data Databases = Databases { stateDB :: S.DB, receiptsDB :: S.DB, expiryDB :: S.DB, auditDB :: S.DB, metaDB :: S.DB }
data Stage = Stage { changes :: M.Map Text (Maybe Value), operations :: [Mutation], effects :: [IO ()] }
data Local = Local ThreadId DB.Txn (IORef Stage)
data Store = Store
 { environment :: DB.Env, databases :: Databases, options :: Options
 , commands :: TQueue Command, health :: TVar (Maybe SomeException), closing :: TVar Bool
 , readers :: TVar (Bool,Int), mapBytes :: IORef Integer, writer :: Async ()
 , author :: MVar (), local :: IORef (Maybe Local), keyLimit :: Int }

integerAtom :: Integer -> Value
integerAtom n = F.NumberAtom n 0
integerValue :: Value -> IO Integer
integerValue (F.NumberAtom n 0) = pure n
integerValue _ = fail "Expected integer metadata"
versionKey :: Integer -> Text
versionKey = T.justifyRight 20 '0' . T.pack . show
keyOf :: Mutation -> Text
keyOf (Set key _) = key
keyOf (Delete key) = key
apply :: M.Map Text Value -> [Mutation] -> M.Map Text Value
apply = foldl' step where
 step kv (Set key value) = M.insert key value kv
 step kv (Delete key) = M.delete key kv
applyChange :: M.Map Text (Maybe Value) -> Mutation -> M.Map Text (Maybe Value)
applyChange kv (Set key value) = M.insert key (Just value) kv
applyChange kv (Delete key) = M.insert key Nothing kv

openStore :: FilePath -> IO Store
openStore = openStoreWith defaultOptions
openStoreWith :: Options -> FilePath -> IO Store
openStoreWith config path = mask_ $ do
 unless (initialMapBytes config >= 131072 && maximumMapBytes config >= initialMapBytes config) $
  fail "Append4: invalid LMDB capacity settings"
 createDirectoryIfMissing True (takeDirectory path)
 lock <- tryLockFile (path <.> "lock") Exclusive >>= maybe (fail "Append4: store already open") pure
 flip onException (unlockFile lock) $ do
  exists <- doesFileExist path
  size <- if exists then getFileSize path else pure 0
  let capacity = min (maximumMapBytes config) (max (initialMapBytes config) (((size*2+4095) `div` 4096)*4096))
  when (size > maximumMapBytes config) $ fail "Append4: database exceeds configured LMDB capacity"
  env <- DB.open path capacity
  flip onException (DB.close env) $ do
   dbs <- runInBoundThread $ DB.write env $ \txn -> do
    dbs <- Databases <$> S.open txn "state" <*> S.open txn "receipts"
      <*> S.open txn "expiry" <*> S.open txn "audit" <*> S.open txn "metadata"
    let format = F.TextAtom ("append4-single-3:"<>T.pack(show(historyPolicy config)))
    previous <- S.get txn (metaDB dbs) "format"
    case previous of
     Nothing -> S.put txn (metaDB dbs) "format" format
     Just saved -> unless (saved == format) $ fail "Append4: incompatible scalar format or history policy"
    pure dbs
#ifndef mingw32_HOST_OS
   -- Make a newly created data-file name durable as well as its contents.
   bracket (Posix.openFd (takeDirectory path) Posix.ReadOnly Posix.defaultFileFlags) Posix.closeFd Posix.fileSynchronise
#endif
   queue <- newTQueueIO
   failed <- newTVarIO Nothing
   stopped <- newTVarIO False
   access <- newTVarIO (False,0)
   capacityRef <- newIORef capacity
   mutex <- newMVar ()
   working <- newIORef Nothing
   limit <- fromIntegral <$> DB.maxKeySize env
   start <- newEmptyMVar
   worker <- asyncBound ((takeMVar start >>= runWriter) `finally` (DB.close env `finally` unlockFile lock))
   let store = Store env dbs config queue failed stopped access capacityRef worker mutex working limit
   putMVar start store
   pure store

ensureOpen :: Store -> STM ()
ensureOpen store = do
 readTVar (health store) >>= mapM_ throwSTM
 readTVar (closing store) >>= flip when (throwSTM (userError "Append4: store closed"))

-- MDB_NOTLS allows a read transaction to follow a Haskell thread between OS
-- threads. Exclusive resize waits for readers; no mapped pointer escapes.
withReader :: Store -> (DB.Txn -> IO a) -> IO a
withReader store action = bracket_ acquire release $
 bracket (DB.begin (environment store) True) DB.abort action
 where
 acquire = atomically $ do
  ensureOpen store
  (resizing,count) <- readTVar (readers store)
  check (not resizing)
  writeTVar (readers store) (False,count+1)
 release = atomically $ modifyTVar' (readers store) (\(busy,n) -> (busy,n-1))

databaseFor :: Store -> Text -> S.DB
databaseFor store key
 | "receipts/" `T.isPrefixOf` key = receiptsDB dbs
 | "expiry/" `T.isPrefixOf` key = expiryDB dbs
 | otherwise = stateDB dbs
 where dbs = databases store
readVersion :: Store -> DB.Txn -> IO Integer
readVersion store txn = S.get txn (metaDB (databases store)) "version" >>= maybe (pure 0) integerValue
readValue :: Store -> DB.Txn -> Text -> IO (Maybe Value)
readValue store txn key = S.get txn (databaseFor store key) (TE.encodeUtf8 key)

-- Caller must exclude every writer (including other processes), or use an
-- immutable backup. This opens read-only without touching data or lock files.
getOfflineValue :: FilePath -> Text -> IO (Maybe Value)
getOfflineValue path key = bracket (DB.openOffline path) DB.close $ \env ->
 bracket (DB.begin env True) DB.abort $ \txn -> do
  let name | "receipts/" `T.isPrefixOf` key = "receipts"
           | "expiry/" `T.isPrefixOf` key = "expiry"
           | otherwise = "state"
  db <- S.existing txn name
  S.get txn db (TE.encodeUtf8 key)
validateKey :: Store -> Text -> IO ()
validateKey _ key = unless (not (T.null key)) $
 fail "Append4: empty KV key"

submit :: Store -> Transaction -> IO Ticket
submit store tx = do
 validateKey store ("id/" <> transactionId tx)
 enqueue store tx Nothing
enqueue :: Store -> Transaction -> Maybe Integer -> IO Ticket
enqueue store tx expected = do
 mapM_ validateMutation (mutations tx)
 mapM_ (mapM_ validateValue . snd) (conditions tx)
 mapM_ (validateKey store) (map keyOf (mutations tx) ++ map fst (conditions tx))
 atomically $ do
  ensureOpen store
  result <- newEmptyTMVar
  writeTQueue (commands store) (Write tx expected result)
  pure result
await :: Ticket -> IO Outcome
await ticket = atomically (readTMVar ticket) >>= either throwIO pure
-- Compatibility barrier: all earlier submissions are already durable when it
-- returns. Applications do not need periodic flush calls.
flush :: Store -> IO ()
flush store = do
 result <- atomically $ do
  ensureOpen store
  reply <- newEmptyTMVar
  writeTQueue (commands store) (Barrier reply)
  pure reply
 atomically (readTMVar result) >>= either throwIO pure
closeStore :: Store -> IO ()
closeStore store = mask_ $ do
 atomically $ do
  stopped <- readTVar (closing store)
  unless stopped $ writeTVar (closing store) True >> writeTQueue (commands store) Close
 wait (writer store)
 readTVarIO (health store) >>= mapM_ throwIO

-- The worker is bound to one OS thread for the entire LMDB write transaction.
-- Map-full retries replay only prepared mutations, never application IO.
runWriter :: Store -> IO ()
runWriter store = loop where
 loop = do
  command <- atomically (readTQueue (commands store))
  case command of
   Close -> atomically $ readTVar (readers store) >>= check . ((==0) . snd)
   Barrier reply -> do
    failure <- readTVarIO (health store)
    atomically (putTMVar reply (maybe (Right ()) Left failure))
    loop
   Write tx expected reply -> do
    result <- try $ do
     readTVarIO (health store) >>= mapM_ throwIO
     commitWithGrowth store tx expected
    case result of
     Left err -> atomically (writeTVar (health store) (Just err))
     Right _ -> pure ()
    atomically (putTMVar reply result)
    loop

commitWithGrowth :: Store -> Transaction -> Maybe Integer -> IO Outcome
commitWithGrowth store tx expected = commitTransaction store tx expected `catch` \(err :: DB.Error) -> do
 old <- readIORef (mapBytes store)
 let next = min (maximumMapBytes (options store)) (old*2)
 if not (DB.isMapFull err) || next <= old then throwIO err else do
  bracket_ (atomically $ modifyTVar' (readers store) (\(_,n) -> (True,n)))
   (atomically $ modifyTVar' (readers store) (\(_,n) -> (False,n))) $ do
    atomically $ readTVar (readers store) >>= check . ((==0) . snd)
    DB.setMapSize (environment store) next
    writeIORef (mapBytes store) next
  commitWithGrowth store tx expected

commitTransaction :: Store -> Transaction -> Maybe Integer -> IO Outcome
commitTransaction store submitted expected = DB.write (environment store) $ \txn -> do
 before <- readVersion store txn
 let meta = metaDB (databases store)
     explicit = isNothing expected
     ident = if explicit then transactionId submitted else "local:" <> T.pack (show (before+1))
     tx = submitted { transactionId = ident }
 duplicate <- if explicit then isJust <$> S.get txn meta (TE.encodeUtf8 ("id/"<>ident)) else pure False
 matches <- and <$> forM (conditions tx) (\(key,value) -> (==value) <$> readValue store txn key)
 original <- M.traverseWithKey (\key _ -> readValue store txn key) (M.fromList [(keyOf op,()) | op <- mutations tx])
 let changed = foldl' applyChange original (mutations tx)
 if maybe False (/=before) expected then pure Conflict
 else if null (mutations tx) then pure Empty
 else if duplicate then pure Duplicate
 else if not matches then pure Conflict
 else if changed == original then pure NoChange
 else do
  when (before >= toInteger (maxBound :: Word64)) $ fail "Append4: version exhausted"
  let number = before+1
  forM_ (M.toList changed) $ \(key,value) -> when (M.lookup key original /= Just value) $
   maybe (S.delete txn (databaseFor store key) (TE.encodeUtf8 key))
    (S.put txn (databaseFor store key) (TE.encodeUtf8 key)) value
  S.put txn meta "version" (integerAtom number)
  when explicit $ S.put txn meta (TE.encodeUtf8 ("id/"<>ident)) (integerAtom number)
  when (historyPolicy (options store) == KeepHistory) $
   writeAppend store txn (Append number [tx])
  pure (Accepted number)

current :: Store -> IO (Maybe (DB.Txn,IORef Stage))
current store = do
 thread <- myThreadId
 working <- readIORef (local store)
 pure $ case working of Just (Local owner txn ref) | owner == thread -> Just (txn,ref); _ -> Nothing

transaction :: Store -> IO a -> IO a
transaction store action = do
 nested <- current store
 case nested of
  Just _ -> action
  Nothing -> mask $ \restore -> do
   (result,callbacks,interrupted) <- withMVar (author store) $ \_ -> do
    (result,before,stage) <- withReader store $ \txn -> do
     before <- readVersion store txn
     thread <- myThreadId
     ref <- newIORef (Stage M.empty [] [])
     writeIORef (local store) (Just (Local thread txn ref))
     flip finally (writeIORef (local store) Nothing) $ do
      result <- restore action
      stage <- readIORef ref
      pure (result,before,stage)
    interrupted <- if null (operations stage) then pure Nothing else do
     ticket <- enqueue store (Transaction "" [] (reverse (operations stage))) (Just before)
     completion <- try (await ticket)
     (outcome,interrupted) <- case completion of
      Right outcome -> pure (outcome,Nothing)
      Left err | Just (_ :: SomeAsyncException) <- fromException err -> do
       outcome <- uninterruptibleMask_ (await ticket)
       pure (outcome,Just err)
      Left err -> throwIO (err :: SomeException)
     case outcome of
      Accepted _ -> pure ()
      NoChange -> pure ()
      _ -> fail ("Append4: transaction rejected: " ++ show outcome)
     pure interrupted
    pure (result,reverse (effects stage),interrupted)
   sequence_ callbacks
   mapM_ throwIO interrupted
   pure result

getValue :: Store -> Text -> IO (Maybe Value)
getValue store key = do
 validateKey store key
 current store >>= maybe (withReader store (\txn -> readValue store txn key))
  (\(txn,ref) -> readIORef ref >>= maybe (readValue store txn key) pure . M.lookup key . changes)
putValue :: Store -> Text -> Value -> IO ()
putValue store key value = mutate store (Set key value)
deleteValue :: Store -> Text -> IO ()
deleteValue store key = mutate store (Delete key)
mutate :: Store -> Mutation -> IO ()
mutate store op = do
 validateMutation op
 validateKey store (keyOf op)
 transaction store $ current store >>= \working -> case working of
  Nothing -> fail "Append4: missing transaction context"
  Just (_,ref) -> modifyIORef' ref $ \stage -> stage
   { changes = applyChange (changes stage) op, operations = op : operations stage }
afterCommit :: Store -> IO () -> IO ()
afterCommit store effect = current store >>= maybe effect
 (\(_,ref) -> modifyIORef' ref (\stage -> stage { effects = effect : effects stage }))

values :: Store -> Text -> IO [(Text,Value)]
values store prefix = viewRange store prefix Nothing (Just prefix) maxBound
rangeValues :: Store -> Text -> Text -> Int -> IO [(Text,Value)]
rangeValues store lower upper = viewRange store lower (Just upper) Nothing
viewRange :: Store -> Text -> Maybe Text -> Maybe Text -> Int -> IO [(Text,Value)]
viewRange store lower upper prefix limit = current store >>= maybe
 (withReader store (\txn -> scanValues store txn lower upper prefix limit))
 (\(txn,ref) -> do
   staged <- changes <$> readIORef ref
   let count = fromInteger (min (toInteger (maxBound :: Int)) (toInteger limit + toInteger (M.size staged)))
   rows <- M.fromList <$> scanValues store txn lower upper prefix count
   let inside key = key >= lower && maybe True (key <) upper && maybe True (`T.isPrefixOf` key) prefix
       overlay = M.filterWithKey (\key _ -> inside key) staged
       merged = M.foldlWithKey' (\kv key value -> maybe (M.delete key kv) (\v -> M.insert key v kv) value) rows overlay
   pure (take limit (M.toAscList merged)))
scanValues :: Store -> DB.Txn -> Text -> Maybe Text -> Maybe Text -> Int -> IO [(Text,Value)]
scanValues store txn lower upper prefix limit = do
 let dbs = databases store
     selected = case prefix of
      Just p | "receipts/" `T.isPrefixOf` p -> [receiptsDB dbs]
      Just p | "expiry/" `T.isPrefixOf` p -> [expiryDB dbs]
      _ -> [stateDB dbs,receiptsDB dbs,expiryDB dbs]
 raw <- concat <$> mapM (\db -> S.scan txn db (TE.encodeUtf8 lower) (TE.encodeUtf8 <$> upper) (TE.encodeUtf8 <$> prefix) limit) selected
 rows <- forM raw $ \(key,value) -> do
  name <- either throwIO pure (TE.decodeUtf8' key)
  pure (name,value)
 pure (take limit (M.toAscList (M.fromList rows)))

readSnapshot :: Store -> IO Snapshot
readSnapshot store = withReader store $ \txn -> Snapshot <$> readVersion store txn
 <*> (M.fromList <$> scanValues store txn "" Nothing Nothing maxBound)
writeAppend :: Store -> DB.Txn -> Append -> IO ()
writeAppend store txn item = forM_ (M.toList (Key.flatten (versionKey(appendId item)) (toJSON item))) $ \(k,v)->
 S.put txn (auditDB(databases store)) (TE.encodeUtf8 k) v
readAppend :: Store -> DB.Txn -> Integer -> IO Append
readAppend store txn number = do
 let root=versionKey number; prefix=TE.encodeUtf8(root<>"/")
 rows<-S.scan txn (auditDB(databases store)) prefix Nothing (Just prefix) maxBound
 v<-Key.unflatten root (M.fromList [(TE.decodeUtf8 k,a)|(k,a)<-rows]) >>= maybe (fail "Missing scalar history") pure
 case fromJSON v of Error e->fail e; Success item->pure item
historyPage :: Store -> Integer -> Int -> IO [Append]
historyPage store after limit = withReader store $ \txn -> do
 latest<-readVersion store txn
 if historyPolicy(options store)==CurrentOnly || limit<=0 then pure [] else
  mapM (readAppend store txn) [max 0 after+1..min latest(max 0 after+toInteger limit)]
history :: Store -> IO [Append]
history store = historyPage store 0 maxBound
snapshotAt :: Store -> Integer -> IO (Maybe Snapshot)
snapshotAt store number = withReader store $ \txn -> do
 latest<-readVersion store txn
 if number<0 || number>latest then pure Nothing
 else if number==latest then Just . Snapshot number . M.fromList <$> scanValues store txn "" Nothing Nothing maxBound
 else if number==0 then pure(Just(Snapshot 0 M.empty))
 else if historyPolicy(options store)==CurrentOnly then pure Nothing else do
  let replay i kv | i>number = pure kv
                  | otherwise = do
                     item<-readAppend store txn i
                     replay (i+1) (foldl' (\s tx -> apply s (mutations tx)) kv (transactions item))
  Just . Snapshot number <$> replay 1 M.empty
isSingleStorage :: FilePath -> IO Bool
isSingleStorage path = bracket (DB.openOffline path) DB.close $ \env ->
 bracket (DB.begin env True) DB.abort $ \txn -> do
  db<-DB.existingDB txn "metadata"
  value<-DB.get txn db "format"
  case value of
   Nothing -> pure False
   Just bytes -> do
    parsed<-try (F.decode bytes) :: IO (Either SomeException Value)
    pure (case parsed of Right(F.TextAtom t)->"append4-single-3:" `T.isPrefixOf` t;_->False)

offlineValues :: FilePath -> Text -> IO [(Text,Value)]
offlineValues path prefix = bracket (DB.openOffline path) DB.close $ \env ->
 bracket (DB.begin env True) DB.abort $ \txn -> do
  rows<-concat <$> forM ["state","receipts","expiry"] (\name->do
   db<-S.existing txn name
   S.scan txn db (TE.encodeUtf8 prefix) Nothing (Just(TE.encodeUtf8 prefix)) maxBound)
  pure[(TE.decodeUtf8 k,v)|(k,v)<-rows]

validateValue :: Value -> IO ()
validateValue (F.ArrayAtom n) | n/=0 = fail "Only scalar or empty-container values are allowed"
validateValue _ = pure ()
validateMutation :: Mutation -> IO ()
validateMutation (Set _ v) = validateValue v
validateMutation (Delete _) = pure ()
