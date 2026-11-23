{-# LANGUAGE ForeignFunctionInterface #-}
-- Small binding to the vendored LMDB API. All returned bytes are copied before
-- the transaction ends; no pointer into a memory map escapes this module.
module Append4.LMDB where

#include "lmdb.h"

import Control.Exception
import Control.Monad
import qualified Data.ByteString as B
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Foreign
import Foreign.C

data Environment
data Transaction
data Cursor
type Env = Ptr Environment
type Txn = Ptr Transaction
type DB = CUInt

data Error = Error CInt String deriving Show
instance Exception Error

foreign import ccall unsafe "mdb_env_create" envCreate :: Ptr Env -> IO CInt
foreign import ccall safe "mdb_env_open" envOpen :: Env -> CString -> CUInt -> CUInt -> IO CInt
foreign import ccall safe "mdb_env_close" close :: Env -> IO ()
foreign import ccall unsafe "mdb_env_set_mapsize" setMapSizeRaw :: Env -> CSize -> IO CInt
foreign import ccall unsafe "mdb_env_set_maxdbs" setMaxDBs :: Env -> CUInt -> IO CInt
foreign import ccall unsafe "mdb_env_set_maxreaders" setMaxReaders :: Env -> CUInt -> IO CInt
foreign import ccall unsafe "mdb_env_get_maxkeysize" maxKeySize :: Env -> IO CInt
foreign import ccall safe "mdb_env_sync" syncRaw :: Env -> CInt -> IO CInt
foreign import ccall safe "mdb_txn_begin" beginRaw :: Env -> Txn -> CUInt -> Ptr Txn -> IO CInt
foreign import ccall unsafe "mdb_txn_abort" abort :: Txn -> IO ()
foreign import ccall safe "mdb_txn_commit" commitRaw :: Txn -> IO CInt
foreign import ccall unsafe "mdb_dbi_open" dbOpenRaw :: Txn -> CString -> CUInt -> Ptr DB -> IO CInt
foreign import ccall safe "mdb_get" getRaw :: Txn -> DB -> Ptr () -> Ptr () -> IO CInt
foreign import ccall safe "mdb_put" putRaw :: Txn -> DB -> Ptr () -> Ptr () -> CUInt -> IO CInt
foreign import ccall safe "mdb_del" deleteRaw :: Txn -> DB -> Ptr () -> Ptr () -> IO CInt
foreign import ccall unsafe "mdb_cursor_open" cursorOpen :: Txn -> DB -> Ptr (Ptr Cursor) -> IO CInt
foreign import ccall unsafe "mdb_cursor_close" cursorClose :: Ptr Cursor -> IO ()
foreign import ccall safe "mdb_cursor_get" cursorGet :: Ptr Cursor -> Ptr () -> Ptr () -> CUInt -> IO CInt
foreign import ccall unsafe "mdb_strerror" errorString :: CInt -> IO CString

check :: CInt -> IO ()
check code = unless (code == 0) $ errorString code >>= peekCString >>= throwIO . Error code

isMapFull :: Error -> Bool
isMapFull (Error code _) = code == #{const MDB_MAP_FULL}

open :: FilePath -> Integer -> IO Env
open path bytes = mask_ $ do
 env <- alloca $ \out -> envCreate out >>= check >> peek out
 flip onException (close env) $ do
  setMapSize env bytes
  setMaxDBs env 32 >>= check
  setMaxReaders env 512 >>= check
  B.useAsCString (TE.encodeUtf8 (T.pack path)) $ \name ->
   envOpen env name (#{const MDB_NOSUBDIR} + #{const MDB_NOTLS}) 0o600 >>= check
  pure env

setMapSize :: Env -> Integer -> IO ()
setMapSize env bytes = do
 when (bytes <= 0 || bytes > toInteger (maxBound :: CSize)) $ fail "Append4: invalid LMDB map size"
 setMapSizeRaw env (fromInteger bytes) >>= check

-- Only for an immutable backup or an externally locked, stopped store. NOLOCK
-- avoids creating/changing lock files while a backup inventory is verified.
openOffline :: FilePath -> IO Env
openOffline path = mask_ $ do
 env <- alloca $ \out -> envCreate out >>= check >> peek out
 flip onException (close env) $ do
  setMaxDBs env 32 >>= check
  B.useAsCString (TE.encodeUtf8 (T.pack path)) $ \name ->
   envOpen env name (#{const MDB_NOSUBDIR} + #{const MDB_NOTLS} + #{const MDB_RDONLY} + #{const MDB_NOLOCK}) 0 >>= check
  pure env

sync :: Env -> IO ()
sync env = syncRaw env 1 >>= check

begin :: Env -> Bool -> IO Txn
begin env readOnly = alloca $ \out -> do
 beginRaw env nullPtr (if readOnly then #{const MDB_RDONLY} else 0) out >>= check
 peek out

commit :: Txn -> IO ()
commit txn = commitRaw txn >>= check

-- The caller keeps a write transaction on one bound OS thread. LMDB consumes
-- the transaction even when commit returns an error: do not abort it twice.
write :: Env -> (Txn -> IO a) -> IO a
write env action = mask_ $ do
 txn <- begin env False
 result <- action txn `onException` abort txn
 commit txn
 pure result

openDB :: Txn -> String -> IO DB
openDB txn name = withCString name $ \key -> alloca $ \out -> do
 dbOpenRaw txn key #{const MDB_CREATE} out >>= check
 peek out

existingDB :: Txn -> String -> IO DB
existingDB txn name = withCString name $ \key -> alloca $ \out -> do
 dbOpenRaw txn key 0 out >>= check
 peek out

withValue :: B.ByteString -> (Ptr () -> IO a) -> IO a
withValue bytes action = B.useAsCStringLen bytes $ \(ptr,len) -> allocaBytes #{size MDB_val} $ \value -> do
 #{poke MDB_val, mv_size} value (fromIntegral len :: CSize)
 #{poke MDB_val, mv_data} value ptr
 action value

copyValue :: Ptr () -> IO B.ByteString
copyValue value = do
 size <- #{peek MDB_val, mv_size} value :: IO CSize
 ptr <- #{peek MDB_val, mv_data} value
 B.packCStringLen (ptr,fromIntegral size)

get :: Txn -> DB -> B.ByteString -> IO (Maybe B.ByteString)
get txn db key = withValue key $ \k -> allocaBytes #{size MDB_val} $ \v -> do
 code <- getRaw txn db k v
 if code == #{const MDB_NOTFOUND} then pure Nothing else check code >> Just <$> copyValue v

put :: Txn -> DB -> B.ByteString -> B.ByteString -> IO ()
put txn db key value = withValue key $ \k -> withValue value $ \v -> putRaw txn db k v 0 >>= check

delete :: Txn -> DB -> B.ByteString -> IO ()
delete txn db key = withValue key $ \k -> do
 code <- deleteRaw txn db k nullPtr
 unless (code == #{const MDB_NOTFOUND}) (check code)

-- Seek into the B-tree and stop at the prefix/range boundary. A prefix read
-- never walks unrelated keys. The limit also bounds relay expiry work.
scan :: Txn -> DB -> B.ByteString -> Maybe B.ByteString -> Maybe B.ByteString -> Int -> IO [(B.ByteString,B.ByteString)]
scan _ _ _ _ _ limit | limit <= 0 = pure []
scan txn db lower upper prefix limit = bracket acquire cursorClose $ \cursor ->
 withValue lower $ \k -> allocaBytes #{size MDB_val} $ \v -> do
  let loop op remaining rows = do
       code <- cursorGet cursor k v op
       if code == #{const MDB_NOTFOUND} then pure (reverse rows) else do
        check code
        key <- copyValue k
        if maybe False (key >=) upper || maybe False (not . (`B.isPrefixOf` key)) prefix then pure (reverse rows) else do
         value <- copyValue v
         let next = (key,value):rows
         if remaining == 1 then pure (reverse next) else loop #{const MDB_NEXT} (remaining-1) next
  loop (if B.null lower then #{const MDB_FIRST} else #{const MDB_SET_RANGE}) limit []
 where
 acquire = alloca $ \out -> cursorOpen txn db out >>= check >> peek out
