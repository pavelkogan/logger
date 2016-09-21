{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE Trustworthy #-}

module System.Log.FastLogger.IO where

import Control.Exception (assert, mask, onException)
import Control.Monad (join)
import Data.ByteString.Builder.Extra (Next(..))
import qualified Data.ByteString.Builder.Extra as BBE
import Data.ByteString.Internal (ByteString(..))
import Data.Word (Word8)
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Alloc (mallocBytes, free)
import Foreign.Ptr (Ptr, plusPtr)
import System.Log.FastLogger.IORef
import System.Log.FastLogger.LogStr

type Buffer = Ptr Word8

-- | The type for buffer size of each core.
type BufSize = Int

-- | The default buffer size (4,096 bytes).
defaultBufSize :: BufSize
defaultBufSize = 4096

getBuffer :: BufSize -> IO Buffer
getBuffer = mallocBytes

freeBuffer :: Buffer -> IO ()
freeBuffer = free

type BufTuple = (Buffer, BufSize, (Buffer -> Int -> IO ()), Builder)
data WriteState = WSNotWriting [BufTuple]
                | WSWriting [BufTuple]

toBufIOWith :: IORef WriteState -> Buffer -> BufSize -> (Buffer -> Int -> IO ()) -> Builder -> IO ()
toBufIOWith wsRef buf !size io builder =
    mask $ \restore -> join $ atomicModifyIORef' wsRef $ \ws ->
        case ws of
            WSWriting rest -> (WSWriting (tuple : rest), return ())
            WSNotWriting rest -> (WSWriting [], loop restore (tuple : rest))
  where
    tuple :: BufTuple
    tuple = (buf, size, io, builder)

    loop :: (IO () -> IO ()) -> [BufTuple] -> IO ()
    loop restore [] =
        join $ atomicModifyIORef' wsRef $ \ws ->
            case ws of
                WSWriting [] -> (WSNotWriting [], return ())
                WSWriting rest -> (WSWriting [], loop restore rest)
                WSNotWriting rest -> assert False (WSNotWriting rest, return ())
    loop restore (tuple':rest) = do
        restore (toBufIOWithInner tuple') `onException`
            atomicModifyIORef' wsRef (\ws ->
                case ws of
                    WSWriting rest' -> (WSNotWriting (rest ++ rest'), ())
                    WSNotWriting rest' -> assert False (WSNotWriting (rest ++ rest'), ()))
        loop restore rest

toBufIOWithInner :: BufTuple -> IO ()
toBufIOWithInner (buf, !size, io, builder) = loop $ BBE.runBuilder builder
  where
    loop writer = do
        (len, next) <- writer buf size
        io buf len
        case next of
             Done -> return ()
             More minSize writer'
               | size < minSize -> error "toBufIOWith: More: minSize"
               | otherwise      -> loop writer'
             Chunk (PS fptr off siz) writer' ->
               withForeignPtr fptr $ \ptr -> io (ptr `plusPtr` off) siz >> loop writer'
