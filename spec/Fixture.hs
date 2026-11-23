module Main where
import qualified Append4 as A
import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception
import Control.Monad
import Data.Aeson
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy.Char8 as L
import Data.IORef
import qualified Data.Map.Strict as M
import Data.Maybe
import qualified Data.Text as T
import GHC.Clock
import System.Directory
import System.Environment
import System.FilePath
import System.IO (hFlush, stdout)
import System.Timeout

main :: IO ()
main = do
 [mode,root] <- getArgs
 createDirectoryIfMissing True root
 let path = root </> "store.lmdb"
 case mode of
  "checks" -> checks path >>= L.putStrLn . encode
  "write" -> do
   store <- A.openStore path
   A.putValue store "role" (String "assistant")
   putStrLn "committed"
   hFlush stdout
   forever (threadDelay 1000000)
  "uncommitted" -> do
   store <- A.openStore path
   A.transaction store $ do
    A.putValue store "role" (String "lost")
    putStrLn "staged"
    hFlush stdout
    forever (threadDelay 1000000)
  "recover" -> bracket (A.openStore path) A.closeStore $ \store -> do
   snap <- A.readSnapshot store
   L.putStrLn (encode (object ["version" .= A.version snap,"role" .= M.lookup "role" (A.entries snap)]))
  "load" -> loadCheck path >>= L.putStrLn . encode
  _ -> fail "Unknown fixture mode"

rejected :: IO a -> IO Bool
rejected action = either (const True) (const False) <$> (try (void action) :: IO (Either SomeException ()))
within :: IO a -> IO a
within action = timeout 10000000 action >>= maybe (fail "Operation failed to complete without a flush clock") pure

checks :: FilePath -> IO Value
checks path = do
 store <- A.openStore path
 let submit name ops = A.submit store (A.Transaction name [] ops) >>= within . A.await
     put name value = submit name [A.Set "role" (String value)]
 before <- A.readSnapshot store
 o1 <- put "one" "assistant"
 pinned <- A.readSnapshot store
 size <- getFileSize path
 A.flush store
 emptySize <- getFileSize path
 rejectedResults <- sequence
  [put "same" "assistant",submit "missing" [A.Delete "absent"],
   submit "round-trip" [A.Set "role" (String "manager"),A.Set "role" (String "assistant")]]
 afterRejected <- A.readSnapshot store
 opposing <- sequence [put "two" "manager",put "three" "assistant"]
 historical <- A.snapshotAt store 1
 past <- A.history store
 page <- A.historyPage store 1 1
 conflict <- A.submit store (A.Transaction "conflict" [("role",Just (String "wrong"))] [A.Set "leak" (Bool True)]) >>= A.await
 duplicate <- put "one" "duplicate"
 ordered1 <- A.submit store (A.Transaction "four" [] [A.Set "role" (String "supervisor")])
 ordered2 <- A.submit store (A.Transaction "five" [("role",Just (String "supervisor"))] [A.Set "checked" (Bool True)])
 ordered <- mapM A.await [ordered1,ordered2]
 -- An independently submitted write invalidates a staged read snapshot.
 staged <- newEmptyMVar
 proceed <- newEmptyMVar
 contender <- async $ rejected $ A.transaction store $ do
  A.putValue store "role" (String "stale")
  putMVar staged ()
  takeMVar proceed
 takeMVar staged
 void (put "concurrent" "winner")
 putMVar proceed ()
 conflictRaised <- wait contender
 winner <- A.getValue store "role"
 effect <- newIORef (0 :: Int)
 rollbackRaised <- rejected $ A.transaction store $ do
  A.putValue store "rolled-back" (Bool True)
  A.afterCommit store (modifyIORef' effect (+1))
  fail "abort"
 rollback <- A.getValue store "rolled-back"
 effectRolledBack <- (==0) <$> readIORef effect
 within $ A.transaction store $ do
  A.putValue store "first" (Bool True)
  A.transaction store (A.putValue store "second" (Bool True))
  A.putValue store "receipts/request" (String "done")
  A.putValue store "expiry/0001/message" (String "first")
  A.afterCommit store $ do
   visible <- A.getValue store "second"
   when (visible == Just (Bool True)) (modifyIORef' effect (+1))
 effectAfter <- (==1) <$> readIORef effect
 A.transaction store $ forM_ ["a","b","c"] $ \key -> A.putValue store ("prefix/"<>key) (String key)
 overlay <- A.transaction store $ do
  A.deleteValue store "prefix/a"
  A.putValue store "prefix/bb" (String "bb")
  A.rangeValues store "prefix/" "prefix0" 2
 prefix <- A.values store "prefix/"
 isolated <- newEmptyMVar
 release <- newEmptyMVar
 held <- async $ A.transaction store $ do
  A.putValue store "not-yet-visible" (Bool True)
  putMVar isolated ()
  takeMVar release
 takeMVar isolated
 hidden <- A.getValue store "not-yet-visible"
 cancel held
 gone <- A.getValue store "not-yet-visible"
 -- Many authors complete with no flush subscriber. Each derives from the
 -- preceding durable value; there is no lost update or heartbeat dependency.
 within $ mapConcurrently_ (\_ -> A.transaction store $ do
  current <- A.getValue store "counter"
  let n = case current of Just (Number value) -> round value; _ -> 0 :: Int
  A.putValue store "counter" (toJSON (n+1))) [1..32 :: Int]
 counter <- A.getValue store "counter"
 exclusive <- rejected (bracket (A.openStore path) A.closeStore (const (pure ())))
 queued <- A.submit store (A.Transaction "close" [] [A.Set "role" (String "closed")])
 A.closeStore store
 closed <- A.await queued
 closedReject <- rejected (A.putValue store "late" Null)
 recovered <- bracket (A.openStore path) A.closeStore A.readSnapshot
 persistentDuplicate <- bracket (A.openStore path) A.closeStore $ \reopened ->
  A.submit reopened (A.Transaction "one" [] [A.Set "role" Null]) >>= A.await
 -- Inspect an immutable backup without changing it or creating lock files.
 let backup = path<.>"backup"
 copyFile path backup
 backupBefore <- B.readFile backup
 offline <- A.getOfflineValue backup "role"
 backupAfter <- B.readFile backup
 offlineLock <- doesFileExist (backup<.>"lock")
 nativeLock <- doesFileExist (backup++"-lock")
 -- Capacity exhaustion must abort the entire transaction and fail closed.
 let small = A.defaultOptions { A.initialMapBytes = 131072, A.maximumMapBytes = 131072 }
 broken <- A.openStoreWith small (path<.>"full")
 failed <- rejected $ A.transaction broken $ do
  A.putValue broken "partial" (Bool True)
  A.putValue broken "large" (String (T.replicate 200000 "x"))
 admissionFailed <- rejected (A.putValue broken "late" Null)
 closeFailed <- rejected (A.closeStore broken)
 intact <- bracket (A.openStore (path<.>"full")) A.closeStore A.readSnapshot
 -- Automatic growth replays prepared writes without re-running application IO.
 growth <- A.openStoreWith small { A.maximumMapBytes = 8*1024*1024 } (path<.>"growth")
 runs <- newIORef (0 :: Int)
 A.transaction growth $ do
  modifyIORef' runs (+1)
  A.putValue growth "large" (String (T.replicate 200000 "y"))
 grew <- A.getValue growth "large"
 runCount <- readIORef runs
 A.closeStore growth
 let ephemeral = A.defaultOptions { A.historyPolicy = A.CurrentOnly, A.initialMapBytes = 1024*1024, A.maximumMapBytes = 8*1024*1024 }
 transient <- A.openStoreWith ephemeral (path<.>"transient")
 forM_ [1..80 :: Int] $ \i -> do
  A.putValue transient "message" (String (T.replicate 100000 (if even i then "x" else "y")))
  A.deleteValue transient "message"
 retained <- A.history transient
 old <- A.snapshotAt transient 1
 live <- A.getValue transient "message"
 A.closeStore transient
 transientBytes <- getFileSize (path<.>"transient")
 policyRejected <- rejected (bracket (A.openStore (path<.>"transient")) A.closeStore (const (pure ())))
 B.writeFile (path<.>"legacy") "not an LMDB database"
 legacyRejected <- rejected (bracket (A.openStore (path<.>"legacy")) A.closeStore (const (pure ())))
 pure $ object
  [ "emptyFlush" .= (size==emptySize)
  , "commitWithoutClock" .= (A.version before==0 && o1==A.Accepted 1)
  , "noChange" .= (rejectedResults==replicate 3 A.NoChange && A.version afterRejected==1)
  , "noCrossTransactionNetting" .= (opposing==[A.Accepted 2,A.Accepted 3] && length past==3)
  , "historicalRead" .= (historical==Just pinned && map A.appendId page==[2])
  , "snapshotIsolation" .= (M.lookup "role" (A.entries pinned)==Just (String "assistant"))
  , "atomicConflict" .= (conflict==A.Conflict && M.notMember "leak" (A.entries recovered))
  , "orderedValidation" .= (ordered==[A.Accepted 4,A.Accepted 5])
  , "duplicate" .= (duplicate==A.Duplicate && persistentDuplicate==A.Duplicate)
  , "concurrentValidation" .= (conflictRaised && winner==Just (String "winner"))
  , "rollback" .= (rollbackRaised && isNothing rollback && effectRolledBack)
  , "afterCommit" .= effectAfter
  , "prefixAndRange" .= (map fst prefix==["prefix/b","prefix/bb","prefix/c"] && map fst overlay==["prefix/b","prefix/bb"])
  , "cancelledAuthor" .= (isNothing hidden && isNothing gone)
  , "concurrentAuthors" .= (counter==Just (toJSON (32 :: Int)))
  , "closeDrains" .= (closed==A.Accepted (A.version recovered) && closedReject)
  , "recovery" .= (M.lookup "first" (A.entries recovered)==Just (Bool True) && M.lookup "receipts/request" (A.entries recovered)==Just (String "done"))
  , "immutableBackupRead" .= (offline==Just (String "closed") && backupBefore==backupAfter && not offlineLock && not nativeLock)
  , "failedCommit" .= (failed && admissionFailed && closeFailed && M.null (A.entries intact))
  , "mapGrowth" .= (grew==Just (String (T.replicate 200000 "y")) && runCount==1)
  , "boundedTransientStorage" .= (null retained && isNothing old && isNothing live && transientBytes<2*1024*1024)
  , "historyPolicy" .= policyRejected
  , "legacyRejected" .= legacyRejected
  , "exclusiveWriter" .= exclusive
  ]

loadCheck :: FilePath -> IO Value
loadCheck path = bracket (A.openStoreWith A.defaultOptions { A.historyPolicy = A.CurrentOnly } path) A.closeStore $ \store -> do
 start <- getMonotonicTimeNSec
 mapConcurrently_ (\n -> A.putValue store (T.pack (show n)) (toJSON n)) [1..200 :: Int]
 end <- getMonotonicTimeNSec
 rows <- A.values store ""
 pure (object ["commits" .= length rows,"seconds" .= ((fromIntegral (end-start) / 1000000000) :: Double)])
