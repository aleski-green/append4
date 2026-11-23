{-# LANGUAGE CPP, ForeignFunctionInterface #-}
-- Embedded append-only KV. The caller supplies flushes; there is no clock here.
module Append4
 ( Store, Snapshot(..), Mutation(..), Transaction(..), Outcome(..), Append(..)
 , openStore, closeStore, submit, flush, await, readSnapshot, snapshotAt, history
 , transaction, getValue, putValue, deleteValue, values, afterCommit ) where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Crypto.Hash.SHA256 (hash)
import Data.Aeson
import Data.Binary.Get (runGet, getWord64be)
import Data.Binary.Put (runPut, putWord64be)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import Data.Foldable (toList)
import Data.IORef
import qualified Data.Map.Strict as M
import qualified Data.Sequence as Q
import Data.Text (Text)
import qualified Data.Text as T
import GHC.Generics (Generic)
import System.Directory
import System.FileLock
import System.FilePath
import System.IO
#ifdef mingw32_HOST_OS
import Foreign.C
foreign import ccall unsafe "_wopen" c_open :: CWString -> CInt -> CInt -> IO CInt
foreign import ccall unsafe "_commit" c_sync :: CInt -> IO CInt
foreign import ccall unsafe "_close" c_close :: CInt -> IO CInt
#else
import qualified System.Posix.IO as P
import qualified System.Posix.Unistd as P
#endif

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
data State = State Snapshot (Q.Seq Append) B.ByteString
type Ticket = TMVar (Either SomeException Outcome)
data Queued = Queued Transaction (Maybe (M.Map Text Value)) Ticket
data Command = Flush (TMVar (Either SomeException ())) | Close (TMVar (Either SomeException ()))
data Local = Local ThreadId (IORef (Snapshot,[Mutation],[IO ()]))
data Store = Store
 { committed :: TVar State, pending :: TQueue Queued, commands :: TQueue Command
 , health :: TVar (Maybe SomeException), closing :: TVar Bool, writer :: Async ()
 , author :: MVar (), local :: IORef (Maybe Local), nonce :: IORef Integer }

apply :: M.Map Text Value -> [Mutation] -> M.Map Text Value
apply = foldl step where
 step kv (Set key value) = M.insert key value kv
 step kv (Delete key) = M.delete key kv

-- Complete records are checked against their predecessor. Only an incomplete
-- final frame is discarded. A complete frame with a bad checksum always fails open.
recover :: Handle -> IO State
recover h = go 0 (State (Snapshot 0 M.empty) Q.empty (B.replicate 32 0)) where
 go offset state@(State snap past previous) = do
  header <- B.hGet h 8
  if B.null header then pure state else if B.length header < 8 then trim offset state else do
   let size = runGet getWord64be (L.fromStrict header)
   when (size == 0 || size > 64*1024*1024) $ fail "Append4: invalid frame size"
   payload <- B.hGet h (fromIntegral size)
   digest <- B.hGet h 32
   if B.length payload /= fromIntegral size || B.length digest /= 32 then trim offset state else do
    unless (digest == hash (previous <> header <> payload)) $ fail "Append4: checksum mismatch"
    item <- either fail pure (eitherDecodeStrict payload)
    unless (appendId item == version snap+1 && not (null (transactions item))) $ fail "Append4: invalid append order"
    let next = Snapshot (appendId item) (foldl (\kv tx -> apply kv (mutations tx)) (entries snap) (transactions item))
    go (offset+8+fromIntegral size+32) (State next (past Q.|> item) digest)
 trim offset state = hSetFileSize h offset >> pure state

syncFile :: FilePath -> IO ()
#ifdef mingw32_HOST_OS
syncFile path = withCWString path $ \name -> bracket (c_open name 32770 0) (void . c_close) $ \fd -> do
 when (fd < 0) $ fail "Append4: cannot open sync descriptor"
 result <- c_sync fd
 unless (result == 0) $ fail "Append4: sync failed"
#else
syncFile path = bracket (P.openFd path P.ReadOnly P.defaultFileFlags) P.closeFd P.fileSynchronise
#endif

openStore :: FilePath -> IO Store
openStore path = mask $ \restore -> do
 createDirectoryIfMissing True (takeDirectory path)
 lock <- tryLockFile (path <.> "lock") Exclusive >>= maybe (fail "Append4: store already open") pure
 flip onException (unlockFile lock) $ do
  h <- openBinaryFile path ReadWriteMode
  flip onException (hClose h) $ do
   state <- restore (recover h)
   hSeek h SeekFromEnd 0
   hFlush h
   syncFile path
#ifndef mingw32_HOST_OS
   syncFile (takeDirectory path)
#endif
   snapshot <- newTVarIO state
   queue <- newTQueueIO
   control <- newTQueueIO
   failed <- newTVarIO Nothing
   stopped <- newTVarIO False
   mutex <- newMVar ()
   working <- newIORef Nothing
   counter <- newIORef 0
   worker <- async (runWriter path h snapshot queue control failed `finally` (hClose h `finally` unlockFile lock))
   pure (Store snapshot queue control failed stopped worker mutex working counter)

ensureOpen :: Store -> STM ()
ensureOpen store = do
 readTVar (health store) >>= mapM_ throwSTM
 readTVar (closing store) >>= flip when (throwSTM (userError "Append4: store closed"))
submit :: Store -> Transaction -> IO Ticket
submit store tx = enqueue store tx Nothing
enqueue :: Store -> Transaction -> Maybe (M.Map Text Value) -> IO Ticket
enqueue store tx expected = atomically $ do
 ensureOpen store
 result <- newEmptyTMVar
 writeTQueue (pending store) (Queued tx expected result)
 pure result
await :: Ticket -> IO Outcome
await ticket = atomically (readTMVar ticket) >>= either throwIO pure
flush :: Store -> IO ()
flush store = do
 result <- atomically $ do
  ensureOpen store
  reply <- newEmptyTMVar
  writeTQueue (commands store) (Flush reply)
  pure reply
 atomically (readTMVar result) >>= either throwIO pure
closeStore :: Store -> IO ()
closeStore store = mask_ $ do
 result <- atomically $ do
  stopped <- readTVar (closing store)
  writeTVar (closing store) True
  if stopped then pure Nothing else do
   reply <- newEmptyTMVar
   writeTQueue (commands store) (Close reply)
   pure (Just reply)
 (forM_ result $ \ticket -> atomically (readTMVar ticket) >>= either throwIO pure) `finally` wait (writer store)
 readTVarIO (health store) >>= mapM_ throwIO

runWriter :: FilePath -> Handle -> TVar State -> TQueue Queued -> TQueue Command -> TVar (Maybe SomeException) -> IO ()
runWriter path h snapshot queue control failed = loop where
 loop = do
  command <- atomically (readTQueue control)
  batch <- atomically (flushTQueue queue)
  result <- try $ do
   readTVarIO failed >>= mapM_ throwIO
   state <- readTVarIO snapshot
   commit state batch
  case result of
   Left err -> atomically $ do
    writeTVar failed (Just err)
    remaining <- flushTQueue queue
    mapM_ (\(Queued _ _ ticket) -> void (tryPutTMVar ticket (Left err))) (batch++remaining)
   Right () -> pure ()
  case command of
   Flush reply -> atomically (putTMVar reply result) >> loop
   Close reply -> atomically (putTMVar reply result)
 commit (State before past previous) batch = do
  let priorIds = M.fromList [(transactionId t,()) | a <- toList past, t <- transactions a]
      choose (kv,ids,accepted,outcomes) (Queued tx expected ticket)
       | maybe False (/=kv) expected = reject Conflict
       | null (mutations tx) = reject Empty
       | M.member (transactionId tx) ids = reject Duplicate
       | any (\(k,v) -> M.lookup k kv /= v) (conditions tx) = reject Conflict
       | after == kv = reject NoChange
       | otherwise = (after,M.insert (transactionId tx) () ids,tx:accepted,(ticket,Accepted (version before+1)):outcomes)
       where after=apply kv (mutations tx)
             reject reason=(kv,ids,accepted,(ticket,reason):outcomes)
      (kv,_,accepted,outcomes) = foldl choose (entries before,priorIds,[],[]) batch
      item = Append (version before+1) (reverse accepted)
      payload = L.toStrict (encode item)
      header = L.toStrict (runPut (putWord64be (fromIntegral (B.length payload))))
      digest = hash (previous <> header <> payload)
  unless (null accepted) $ do
   when (B.length payload > 64*1024*1024) $ fail "Append4: batch exceeds 64 MiB"
   B.hPut h (header <> payload <> digest)
   hFlush h
   syncFile path
  atomically $ do
   unless (null accepted) $ writeTVar snapshot (State (Snapshot (appendId item) kv) (past Q.|> item) digest)
   forM_ outcomes $ \(ticket,result) -> putTMVar ticket (Right result)

readSnapshot :: Store -> IO Snapshot
readSnapshot store = do
 readTVarIO (health store) >>= mapM_ throwIO
 State snap _ _ <- readTVarIO (committed store)
 pure snap
snapshotAt :: Store -> Integer -> IO (Maybe Snapshot)
snapshotAt store number = do
 State current past _ <- readTVarIO (committed store)
 pure $ if number<0 || number>version current then Nothing else Just
  (Snapshot number (foldl (\kv item -> foldl (\state tx -> apply state (mutations tx)) kv (transactions item)) M.empty (takeWhile ((<=number) . appendId) (toList past))))
history :: Store -> IO [Append]
history store = do State _ past _ <- readTVarIO (committed store); pure (toList past)

-- A host transaction stages all nested writes and discards them on exceptions.
-- Reads from other threads see only committed snapshots. The author lock spans
-- acknowledgment, so a second host transaction cannot derive from stale values.
current :: Store -> IO (Maybe (IORef (Snapshot,[Mutation],[IO ()])))
current store = do
 thread <- myThreadId
 working <- readIORef (local store)
 pure $ case working of Just (Local owner ref) | owner == thread -> Just ref; _ -> Nothing
transaction :: Store -> IO a -> IO a
transaction store action = do
 nested <- current store
 case nested of
  Just _ -> action
  Nothing -> do
   (result,effects) <- withMVar (author store) $ \_ -> mask $ \restore -> do
    atomically (ensureOpen store)
    snap <- readSnapshot store
    thread <- myThreadId
    ref <- newIORef (snap,[],[])
    writeIORef (local store) (Just (Local thread ref))
    let reset = writeIORef (local store) Nothing
    (do result <- restore action
        (_,ops,effects) <- readIORef ref
        unless (null ops) $ do
         serial <- atomicModifyIORef' (nonce store) (\n -> (n+1,n+1))
         ticket <- enqueue store (Transaction (T.pack (show (version snap) ++ ":" ++ show serial)) [] (reverse ops)) (Just (entries snap))
         outcome <- await ticket
         case outcome of
          Accepted _ -> pure ()
          NoChange -> pure ()
          _ -> fail ("Append4: transaction rejected: "++show outcome)
        reset
        pure (result,reverse effects)) `onException` reset
   sequence_ effects
   pure result
getValue :: Store -> Text -> IO (Maybe Value)
getValue store key = M.lookup key . entries <$> view store
view :: Store -> IO Snapshot
view store = current store >>= maybe (readSnapshot store) (fmap (\(s,_,_) -> s) . readIORef)
values :: Store -> Text -> IO [(Text,Value)]
values store prefix = filter (T.isPrefixOf prefix . fst) . M.toAscList . entries <$> view store
putValue :: Store -> Text -> Value -> IO ()
putValue store key value = mutate store (Set key value)
deleteValue :: Store -> Text -> IO ()
deleteValue store key = mutate store (Delete key)
mutate :: Store -> Mutation -> IO ()
mutate store op = transaction store $ do
 Just ref <- current store
 modifyIORef' ref $ \(s,ops,effects) -> (s {entries=apply (entries s) [op]},op:ops,effects)
afterCommit :: Store -> IO () -> IO ()
afterCommit store effect = current store >>= maybe effect (\ref -> modifyIORef' ref (\(s,ops,es) -> (s,ops,effect:es)))
