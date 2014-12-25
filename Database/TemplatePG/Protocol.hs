-- Copyright 2010, 2011, 2012, 2013 Chris Forno

-- |The Protocol module allows for direct, low-level communication with a
--  PostgreSQL server over TCP/IP. You probably don't want to use this module
--  directly.

module Database.TemplatePG.Protocol ( PGConnection
                                    , PGException(..)
                                    , pgConnect
                                    , pgDisconnect
                                    , describeStatement
                                    , executeSimpleQuery
                                    , executeSimpleStatement
                                    ) where

import Database.TemplatePG.Types

import Control.Applicative ((<$>))
import Control.Exception
import Control.Monad (liftM, liftM2, replicateM, when)
import Data.Binary
import qualified Data.Binary.Builder as B
import qualified Data.Binary.Get as G
import qualified Data.Binary.Put as P
import Data.ByteString.Internal (c2w, w2c)
import qualified Data.ByteString.Lazy.Char8 as B8
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.UTF8 as U
import qualified Data.Map.Strict as Map
import Data.Maybe (isJust)
import Data.Monoid
import Data.Typeable
import Network (HostName, PortID, connectTo)
import System.Environment (lookupEnv)
import System.IO hiding (putStr, putStrLn)

import Prelude hiding (putStr, putStrLn)

data PGConnection = PGConnection
  { pgHandle :: Handle
  , pgDebug :: !Bool
  , pgPid :: !Word32
  , pgKey :: !Word32
  , pgParameters :: Map.Map L.ByteString L.ByteString
  }

-- |PGMessage represents a PostgreSQL protocol message that we'll either send
--  or receive. See
--  <http://www.postgresql.org/docs/current/interactive/protocol-message-formats.html>.
data PGMessage = Authentication
               | BackendKeyData Word32 Word32
               -- |CommandComplete is bare for now, although it could be made
               --  to contain the number of rows affected by statements in a
               --  later version.
               | CommandComplete
               -- |Each DataRow (result of a query) is a list of ByteStrings
               --  (or just Nothing for null values, to distinguish them from
               --  emtpy strings). The ByteStrings can then be converted to
               --  the appropriate type by 'pgStringToType'.
               | DataRow [Maybe L.ByteString]
               -- |Describe a SQL query/statement. The SQL string can contain
               --  parameters ($1, $2, etc.).
               | Describe String
               | EmptyQueryResponse
               -- |An ErrorResponse contains the severity, "SQLSTATE", and
               --  message of an error. See
               --  <http://www.postgresql.org/docs/current/interactive/protocol-error-fields.html>.
               | ErrorResponse String String String
               | Execute
               | Flush
               | NoData
               | NoticeResponse
               -- |A ParameterDescription describes the type of a given SQL
               --  query/statement parameter ($1, $2, etc.). Unfortunately,
               --  PostgreSQL does not give us nullability information for the
               --  parameter.
               | ParameterDescription [PGType]
               | ParameterStatus L.ByteString L.ByteString
               -- |Parse SQL Destination (prepared statement)
               | Parse String String
               | ParseComplete
               | ReadyForQuery
               -- |A RowDescription contains the name, type, table OID, and
               --  column number of the resulting columns(s) of a query. The
               --  column number is useful for inferring nullability.
               | RowDescription [(String, PGType, Integer, Int)]
               -- |SimpleQuery takes a simple SQL string. Parameters ($1, $2,
               --  etc.) aren't allowed.
               | SimpleQuery String
               | UnknownMessage Word8
  deriving (Show)

-- |PGException is thrown upon encountering an 'ErrorResponse' with severity of
--  ERROR, FATAL, or PANIC. It holds the SQLSTATE and message of the error.
data PGException = PGException String String
  deriving (Show, Typeable)

instance Exception PGException

protocolVersion :: Word32
protocolVersion = 0x30000

-- |Connect to a PostgreSQL server.
pgConnect :: HostName  -- ^ the host to connect to
          -> PortID    -- ^ the port to connect on
          -> String    -- ^ the database to connect to
          -> String    -- ^ the username to connect as
          -> String    -- ^ the password to connect with
          -> IO PGConnection -- ^ a handle to communicate with the PostgreSQL server on
pgConnect host port db user pass = do
  debug <- isJust <$> lookupEnv "TPG_DEBUG"
  h <- connectTo host port
  L.hPut h $ B.toLazyByteString $ pgMessage handshake
  hFlush h
  conn (PGConnection h debug 0 0 Map.empty)
 -- These are here since the handshake message differs a bit from other
 -- messages (it's missing the inital identifying character). I could probably
 -- get rid of it with some refactoring.
 where handshake = mconcat
                     [ B.putWord32be protocolVersion
                     , pgString "user", pgString user
                     , pgString "database", pgString db
                     , B.singleton 0 ]
       pgMessage :: B.Builder -> B.Builder
       pgMessage msg = B.append len msg
         where len = B.putWord32be $ fromIntegral $ (L.length $ B.toLazyByteString msg) + 4
       conn c = do
         m <- pgReceive c
         case m of
           ReadyForQuery -> return c
           BackendKeyData p k -> conn c{ pgPid = p, pgKey = k }
           ParameterStatus k v -> conn c{ pgParameters = Map.insert k v $ pgParameters c }
           _ -> throwIO $ PGException "connect" $ "unhandled message: " ++ show m

-- |Disconnect from a PostgreSQL server. Note that this currently doesn't send
-- a close message.
pgDisconnect :: PGConnection -- ^ a handle from 'pgConnect'
             -> IO ()
pgDisconnect PGConnection{ pgHandle = h } = hClose h

-- |Convert a string to a NULL-terminated UTF-8 string. The PostgreSQL
--  protocol transmits most strings in this format.
-- I haven't yet found a function for doing this without requiring manual
-- memory management.
pgString :: String -> B.Builder
pgString s = B.fromLazyByteString (U.fromString s) <> B.singleton 0

pgMessageID :: PGMessage -> Word8
pgMessageID (UnknownMessage t) = t
pgMessageID m = c2w $ case m of
                  Authentication           -> 'R'
                  (BackendKeyData _ _)     -> 'K'
                  CommandComplete          -> 'C'
                  (DataRow _)              -> 'D'
                  (Describe _)             -> 'D'
                  EmptyQueryResponse       -> 'I'
                  (ErrorResponse _ _ _)    -> 'E'
                  Execute                  -> 'E'
                  Flush                    -> 'H'
                  NoData                   -> 'n'
                  NoticeResponse           -> 'N'
                  (ParameterDescription _) -> 't'
                  (ParameterStatus _ _)    -> 'S'
                  (Parse _ _)              -> 'P'
                  ParseComplete            -> '1'
                  ReadyForQuery            -> 'Z'
                  (RowDescription _)       -> 'T'
                  (SimpleQuery _)          -> 'Q'
                  (UnknownMessage _)       -> error "Unknown message type"

-- |All PostgreSQL messages have a common header: an identifying character and
-- a 32-bit size field.
instance Binary PGMessage where
  -- |Putting a message automatically adds the necessary message type and
  -- message size fields.
  put m = do
    let body = B.toLazyByteString $ putMessageBody m
    P.putWord8 $ pgMessageID m
    P.putWord32be $ fromIntegral $ (L.length body) + 4
    P.putLazyByteString body
  -- |Getting a message takes care of reading the message type and message size
  -- and ensures that just the necessary amount is read and given to
  -- 'getMessageBody' (so that if a 'getMessageBody' parser doesn't read the
  -- entire message it doesn't leave data to interfere with later messages).
  get = do
    (typ, len) <- getMessageHeader
    body <- G.getLazyByteString ((fromIntegral len) - 4)
    return $ G.runGet (getMessageBody typ) body

-- |Given a message, build the over-the-wire representation of it. Note that we
-- send fewer messages than we receive.
putMessageBody :: PGMessage -> B.Builder
putMessageBody (Describe n)    = mconcat [B.singleton $ c2w 'S', pgString n]
putMessageBody Execute         = mconcat [pgString "", B.putWord32be 0]
putMessageBody Flush           = B.empty
putMessageBody (Parse s n)     = mconcat [pgString n, pgString s, B.putWord16be 0]
putMessageBody (SimpleQuery s) = pgString s
putMessageBody _               = undefined

-- |Get the type and size of an incoming message.
getMessageHeader :: Get (Word8, Int)
getMessageHeader = do
  typ <- G.getWord8
  len <- G.getWord32be
  return (typ, fromIntegral len)

-- |Parse an incoming message.
getMessageBody :: Word8 -- ^ the type of the message to parse
               -> Get PGMessage
getMessageBody typ =
    case w2c typ of
      'R' -> do return Authentication
      't' -> do numParams <- fromIntegral `liftM` G.getWord16be
                ps <- replicateM numParams readParam
                return $ ParameterDescription ps
              where readParam = do typ' <- fromIntegral `liftM` G.getWord32be
                                   return $ pgTypeFromOID typ'
      'T' -> do numFields <- fromIntegral `liftM` G.getWord16be
                ds <- replicateM numFields readField
                return $ RowDescription ds
              where readField = do name <- U.toString `liftM` G.getLazyByteStringNul
                                   oid <- fromIntegral `liftM` G.getWord32be -- table OID
                                   col <- fromIntegral `liftM` G.getWord16be -- column number
                                   typ' <- fromIntegral `liftM` G.getWord32be -- type
                                   _ <- G.getWord16be -- type size
                                   _ <- G.getWord32be -- type modifier
                                   _ <- G.getWord16be -- format code
                                   return (name, pgTypeFromOID typ', oid, col)
      'Z' -> G.getWord8 >> return ReadyForQuery
      '1' -> return ParseComplete
      'C' -> return CommandComplete
      'S' -> liftM2 ParameterStatus G.getLazyByteStringNul G.getLazyByteStringNul
      'D' -> do numFields <- fromIntegral `liftM` G.getWord16be
                ds <- replicateM numFields readField
                return $ DataRow ds
              where readField = do len <- fromIntegral `liftM` G.getWord32be
                                   s <- case len of
                                          0xFFFFFFFF -> return Nothing
                                          _          -> Just `liftM` G.getLazyByteString len
                                   return s
      'K' -> liftM2 BackendKeyData G.getWord32be G.getWord32be
      'E' -> do fs <- readFields
                case (lookup (c2w 'S') fs,
                      lookup (c2w 'C') fs,
                      lookup (c2w 'M') fs) of
                  (Just s, Just c, Just m) -> return $ ErrorResponse s c m
                  _                        -> error "Unreadable error response"
              where readFields :: Get [(Word8, String)]
                    readFields = do f <- G.getWord8
                                    case f of
                                      0 -> return []
                                      _ -> do s <- G.getLazyByteStringNul
                                              f' <- readFields
                                              return ((f,U.toString s):f')
      'I' -> return EmptyQueryResponse
      'n' -> return NoData
      'N' -> return NoticeResponse -- Ignore the notice body for now.
      _   -> return $ UnknownMessage typ

-- |Send a message to PostgreSQL (low-level).
pgSend :: PGConnection -> PGMessage -> IO ()
pgSend PGConnection{ pgHandle = h, pgDebug = d } msg = do
  when d $ B8.putStrLn (encode msg)
  L.hPut h (encode msg) >> hFlush h

-- |Receive the next message from PostgreSQL (low-level). Note that this will
-- block until it gets a message.
pgReceive :: PGConnection -> IO PGMessage
pgReceive PGConnection{ pgHandle = h, pgDebug = d } = do
  (typ, len) <- G.runGet getMessageHeader `liftM` L.hGet h 5
  body <- L.hGet h (len - 4)
  when d $ do
            L.putStr (P.runPut (P.putWord8 typ >> P.putWord32be (fromIntegral len)))
            B8.putStrLn body
            hFlush stdout
  let msg = decode $ L.cons typ (L.append (B.toLazyByteString $ B.putWord32be $ fromIntegral len) body)
  case msg of
    (ErrorResponse _ c m) -> throwIO (PGException c m)
    _                     -> return msg

-- |Wait for a message of a given type.
pgWaitFor :: PGConnection
          -> [Word8] -- ^ A list of message identifiers, the first of which
                     -- found while reading messages from PostgreSQL will be
                     -- returned.
          -> IO PGMessage
pgWaitFor h ids = do
  response <- pgReceive h
  if pgMessageID response `elem` ids
    then return response
    else pgWaitFor h ids

-- |Describe a SQL statement/query. A statement description consists of 0 or
-- more parameter descriptions (a PostgreSQL type) and zero or more result
-- field descriptions (for queries) (consist of the name of the field, the
-- type of the field, and a nullability indicator).
describeStatement :: PGConnection
                  -> String -- ^ SQL string
                  -> IO ([PGType], [(String, PGType, Bool)]) -- ^ a list of parameter types, and a list of result field names, types, and nullability indicators.
describeStatement h sql = do
  pgSend h $ Parse sql ""
  pgSend h $ Describe ""
  pgSend h $ Flush
  _ <- pgWaitFor h [pgMessageID ParseComplete]
  (ParameterDescription ps) <- pgReceive h
  m <- pgWaitFor h $ map c2w ['n', 'T']
  case m of
    NoData             -> return (ps, [])
    (RowDescription r) -> do
      r' <- zipWith (\ (name, typ, _, _) n -> (name, typ, n)) r `liftM` mapM nullable r
      return (ps, r')
    _                  -> error ""
 where
   nullable (_, _, oid, col) =
     -- We don't get nullability indication from PostgreSQL, at least not
     -- directly.
     if oid == 0
       -- Without any hints, we have to assume that the result can be null and
       -- leave it up to the developer to figure it out.
       then return True
       -- In cases where the resulting field is tracable to the column of a
       -- table, we can check there.
       else do r <- executeSimpleQuery ("SELECT attnotnull FROM pg_attribute WHERE attrelid = " ++ show oid ++ " AND attnum = " ++ show col) h
               case r of
                 [[Just s]] -> return $ case U.toString s of
                                          "t" -> False
                                          "f" -> True
                                          _   -> error "Unexpected result from PostgreSQL"
                 _          -> error $ "Can't determine nullability of column #" ++ show col

-- |A simple query is one which requires sending only a single 'SimpleQuery'
-- message to the PostgreSQL server. The query is sent as a single string; you
-- cannot bind parameters. Note that queries can return 0 results (an empty
-- list).
executeSimpleQuery :: String                    -- ^ SQL string
                   -> PGConnection
                   -> IO ([[Maybe L.ByteString]]) -- ^ A list of result rows,
                                                -- which themselves are a list
                                                -- of fields.
executeSimpleQuery sql h = do
  pgSend h $ SimpleQuery sql
  m <- pgWaitFor h $ map c2w ['C', 'I', 'T']
  case m of
    EmptyQueryResponse -> return [[]]
    (RowDescription _) -> readDataRows
    _                  -> error "executeSimpleQuery: Unexpected Message"
 where readDataRows = do
         m <- pgWaitFor h $ map c2w ['C', 'D']
         case m of
           CommandComplete -> return []
           (DataRow fs)    -> do rs <- readDataRows
                                 return (fs:rs)
           _               -> error ""

-- |While not strictly necessary, this can make code a little bit clearer. It
-- executes a 'SimpleQuery' but doesn't look for results.
executeSimpleStatement :: String -- ^ SQL string
                       -> PGConnection
                       -> IO ()
executeSimpleStatement sql h = do
  pgSend h $ SimpleQuery sql
  m <- pgWaitFor h $ map c2w ['C', 'I']
  case m of
    CommandComplete    -> return ()
    EmptyQueryResponse -> return ()
    _                  -> error "executeSimpleStatement: Unexpected Message"
