module Main where
import qualified Append4 as A
import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.Aeson
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy.Char8 as L
import qualified Data.Map.Strict as M
import Data.Maybe
import GHC.Conc (threadStatus, ThreadStatus(..), BlockReason(..))
import System.Directory
import System.Environment
import System.FilePath
import System.IO (hFlush, stdout)

main :: IO ()
main = do
 [mode,root] <- getArgs
 createDirectoryIfMissing True root
 let path=root </> "store.append4"
 case mode of
  "checks" -> checks path >>= L.putStrLn . encode
  "write" -> do
   store <- A.openStore path
   ticket <- A.submit store (A.Transaction "committed" [] [A.Set "role" (String "assistant")])
   A.flush store
   void (A.await ticket)
   putStrLn "committed"
   hFlush stdout
   -- The test harness kills this process without closeStore.
   forever (threadDelay 1000000)
  "uncommitted" -> do
   store <- A.openStore path
   void (A.submit store (A.Transaction "uncommitted" [] [A.Set "role" (String "lost")]))
   putStrLn "queued"
   hFlush stdout
   forever (threadDelay 1000000)
  "recover" -> bracket (A.openStore path) A.closeStore $ \store -> do
   snap <- A.readSnapshot store
   L.putStrLn (encode (object ["version" .= A.version snap,"role" .= M.lookup "role" (A.entries snap)]))
  _ -> fail "Unknown fixture mode"

checks :: FilePath -> IO Value
checks path = do
 store <- A.openStore path
 let tx name ops=A.Transaction name [] ops
     put name value=A.submit store (tx name [A.Set "role" (String value)])
     rejected action = isLeft <$> (try action :: IO (Either SomeException ()))
     isLeft (Left _) = True
     isLeft _ = False
 initialSize <- getFileSize path
 A.flush store
 emptySize <- getFileSize path
 t1 <- put "one" "assistant"
 before <- A.readSnapshot store
 unacknowledged <- atomically (isEmptyTMVar t1)
 A.flush store
 o1 <- A.await t1
 pinned <- A.readSnapshot store
 same <- put "same" "assistant"
 missing <- A.submit store (tx "missing" [A.Delete "absent"])
 roundTrip <- A.submit store (tx "cancel" [A.Set "role" (String "manager"),A.Set "role" (String "assistant")])
 A.flush store
 rejectedResults <- mapM A.await [same,missing,roundTrip]
 afterRejected <- A.readSnapshot store
 opposing1 <- put "two" "manager"
 opposing2 <- put "three" "assistant"
 A.flush store
 opposing <- mapM A.await [opposing1,opposing2]
 history <- A.history store
 historical <- A.snapshotAt store 1
 absentVersion <- A.snapshotAt store 99
 conflict <- A.submit store (A.Transaction "conflict" [("role",Just (String "wrong"))] [A.Set "leak" (Bool True),A.Delete "role"])
 ordered1 <- put "four" "supervisor"
 ordered2 <- A.submit store (A.Transaction "five" [("role",Just (String "supervisor"))] [A.Set "checked" (Bool True)])
 A.flush store
 c <- A.await conflict
 order <- mapM A.await [ordered1,ordered2]
 valid <- A.readSnapshot store
 duplicate <- put "one" "duplicate"
 A.flush store
 d <- A.await duplicate
 staged <- newEmptyMVar
 proceed <- newEmptyMVar
 contender <- async $ rejected $ A.transaction store $ do
  A.putValue store "role" (String "stale")
  putMVar staged ()
  takeMVar proceed
 takeMVar staged
 concurrent <- put "concurrent" "winner"
 A.flush store
 void (A.await concurrent)
 putMVar proceed ()
 let settle task = poll task >>= maybe (A.flush store >> threadDelay 1000 >> settle task) (const (pure ()))
 settle contender
 conflictRaised <- wait contender
 winner <- A.getValue store "role"
 effect <- newEmptyMVar
 failed <- rejected $ A.transaction store $ do
  A.putValue store "rolled-back" (Bool True)
  A.afterCommit store (putMVar effect ())
  fail "abort"
 rollback <- A.getValue store "rolled-back"
 effectRolledBack <- isEmptyMVar effect
 finished <- async $ A.transaction store $ do
  A.putValue store "first" (Bool True)
  A.putValue store "second" (Bool True)
  A.afterCommit store (putMVar effect ())
 -- Wait for the submitter to reach its acknowledgement wait, using an explicit
 -- observation signal from the staged transaction instead of a timing assertion.
 let finish = do
      result <- poll finished
      case result of Just _ -> pure (); Nothing -> A.flush store >> threadDelay 1000 >> finish
 finish
 wait finished
 effectsAfter <- not <$> isEmptyMVar effect
 -- Cancel precisely at the queued acknowledgement wait: it must settle the
 -- predecessor and run its callbacks before another author reads the state.
 ready <- newEmptyMVar
 dispatched <- newEmptyMVar
 cancelled <- async $ A.transaction store $ do
  A.putValue store "cancelled-waiter" (Bool True)
  A.afterCommit store (putMVar dispatched ())
  putMVar ready ()
 takeMVar ready
 let queuedWait = threadStatus (asyncThreadId cancelled) >>= \status ->
      case status of ThreadBlocked BlockedOnSTM -> pure (); _ -> yield >> queuedWait
 queuedWait
 cancel cancelled
 settled <- A.getValue store "cancelled-waiter"
 dispatchedOnce <- not <$> isEmptyMVar dispatched
 next <- async (A.transaction store (A.putValue store "after-cancellation" (Bool True)))
 settle next
 wait next
 queued <- put "close" "closed"
 A.closeStore store
 closedResult <- A.await queued
 closedReject <- rejected (void (put "late" "late"))
 recovered <- bracket (A.openStore path) A.closeStore A.readSnapshot
 B.appendFile path (B.pack [0,0,0])
 tailRecovered <- bracket (A.openStore path) A.closeStore A.readSnapshot
 bytes <- B.readFile path
 let corrupt=path<.>"corrupt"
 B.writeFile corrupt (B.take 15 bytes <> B.singleton (B.index bytes 15+1) <> B.drop 16 bytes)
 corruptRejected <- rejected (bracket (A.openStore corrupt) A.closeStore (const (pure ())))
 -- A failed sync must never publish or acknowledge; further writes fail closed.
 let brokenPath=path<.>"sync-failure"
 broken <- A.openStore brokenPath
 pending <- A.submit broken (tx "failed-sync" [A.Set "unpublished" (Bool True)])
 renameFile brokenPath (brokenPath<.>"moved")
 syncFailed <- rejected (A.flush broken)
 ackFailed <- rejected (void (A.await pending))
 admissionFailed <- rejected (void (A.submit broken (tx "late" [A.Delete "unpublished"])))
 closedFailed <- rejected (A.closeStore broken)
 -- Two independent writers may never open the same journal.
 owner <- A.openStore (path<.>"exclusive")
 exclusive <- rejected (bracket (A.openStore (path<.>"exclusive")) A.closeStore (const (pure ())))
 A.closeStore owner
 pure $ object
  [ "emptyFlush" .= (initialSize==emptySize)
  , "publication" .= (A.version before==0 && unacknowledged && o1==A.Accepted 1)
  , "noChange" .= (rejectedResults==replicate 3 A.NoChange && A.version afterRejected==1)
  , "noCrossTransactionNetting" .= (opposing==replicate 2 (A.Accepted 2) && map (length . A.transactions) history==[1,2])
  , "historicalRead" .= (historical==Just pinned && absentVersion==Nothing)
  , "snapshotIsolation" .= (M.lookup "role" (A.entries pinned)==Just (String "assistant") && A.version pinned==1)
  , "atomicConflict" .= (c==A.Conflict && M.notMember "leak" (A.entries valid))
  , "orderedValidation" .= (order==replicate 2 (A.Accepted 3) && M.lookup "checked" (A.entries valid)==Just (Bool True))
  , "duplicate" .= (d==A.Duplicate)
  , "concurrentValidation" .= (conflictRaised && winner==Just (String "winner"))
  , "rollback" .= (failed && isNothing rollback && effectRolledBack)
  , "afterCommit" .= effectsAfter
  , "cancelledWaiter" .= (settled==Just (Bool True) && dispatchedOnce)
  , "closeDrains" .= (closedResult==A.Accepted (A.version recovered) && M.lookup "role" (A.entries recovered)==Just (String "closed") && closedReject)
  , "recovery" .= (M.lookup "first" (A.entries recovered)==Just (Bool True) && M.lookup "second" (A.entries recovered)==Just (Bool True))
  , "incompleteTail" .= (tailRecovered==recovered)
  , "corruption" .= corruptRejected
  , "syncFailure" .= (syncFailed && ackFailed && admissionFailed && closedFailed)
  , "exclusiveWriter" .= exclusive
  ]
