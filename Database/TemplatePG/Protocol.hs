{-# LANGUAGE CPP, DeriveDataTypeable, PatternGuards #-}
-- Copyright 2010, 2011, 2012, 2013 Chris Forno

-- |The Protocol module allows for direct, low-level communication with a
--  PostgreSQL server over TCP/IP. You probably don't want to use this module
--  directly.

module Database.TemplatePG.Protocol ( PGConnection
                                    , PGData
                                    , PGError(..)
                                    , messageCode
                                    , pgConnect
                                    , pgDisconnect
                                    , pgDescribe
                                    , pgSimpleQuery
                                    , pgAddType
                                    , getTypeOID
                                    ) where

import Database.TemplatePG.Types

import Control.Applicative ((<$>), (<$))
import Control.Arrow (second)
import Control.Exception (Exception, throwIO, catch)
import Control.Monad (liftM2, replicateM, when)
#ifdef USE_MD5
import qualified Crypto.Hash as Hash
#endif
import qualified Data.Binary.Get as G
import qualified Data.ByteString.Builder as B
import Data.ByteString.Internal (c2w, w2c)
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.Char8 as LC
import qualified Data.ByteString.Lazy.UTF8 as U
import Data.Foldable (foldMap)
import Data.IORef (IORef, newIORef, writeIORef, readIORef)
import qualified Data.Map as Map
import Data.Maybe (isJust, fromMaybe)
import Data.Monoid (mempty, (<>))
import Data.Typeable (Typeable)
import Data.Word (Word8, Word32)
import Network (HostName, PortID, connectTo)
import System.Environment (lookupEnv)
import System.IO (Handle, hFlush, hClose, stderr, hPutStrLn)
import Text.Read (readMaybe)

data PGState
  = StateUnknown
  | StateIdle
  | StateTransaction
  | StateTransactionFailed
  deriving (Show, Eq)

data PGConnection = PGConnection
  { connHandle :: Handle
  , connDebug :: !Bool
  , connLogMessage :: MessageFields -> IO ()
  , connPid :: !Word32
  , connKey :: !Word32
  , connParameters :: Map.Map String String
  , connTypes :: PGTypeMap
  , connState :: IORef PGState
  }

data ColDescription = ColDescription
  { colName :: String
  , colTable :: !OID
  , colNumber :: !Int
  , colType :: !OID
  } deriving (Show)

-- |A list of (nullable) data values, e.g. a single row or query parameters.
type PGData = [Maybe L.ByteString]

type MessageFields = Map.Map Word8 L.ByteString

-- |PGFrontendMessage represents a PostgreSQL protocol message that we'll send.
-- See <http://www.postgresql.org/docs/current/interactive/protocol-message-formats.html>.
data PGFrontendMessage
  = StartupMessage [(String, String)] -- only sent first
  | CancelRequest Word32 Word32 -- sent first on separate connection
  | Bind { statementName :: String, bindParameters :: PGData }
  | Close { statementName :: String }
  -- |Describe a SQL query/statement. The SQL string can contain
  --  parameters ($1, $2, etc.).
  | Describe { statementName :: String }
  | Execute Word32
  | Flush
  -- |Parse SQL Destination (prepared statement)
  | Parse { statementName :: String, queryString :: String, parseTypes :: [OID] }
  | PasswordMessage L.ByteString
  -- |SimpleQuery takes a simple SQL string. Parameters ($1, $2,
  --  etc.) aren't allowed.
  | SimpleQuery { queryString :: String }
  | Sync
  | Terminate
  deriving (Show)

-- |PGBackendMessage represents a PostgreSQL protocol message that we'll receive.
-- See <http://www.postgresql.org/docs/current/interactive/protocol-message-formats.html>.
data PGBackendMessage
  = AuthenticationOk
  | AuthenticationCleartextPassword
  | AuthenticationMD5Password L.ByteString
  -- AuthenticationSCMCredential
  | BackendKeyData Word32 Word32
  | BindComplete
  | CloseComplete
  -- |CommandComplete is bare for now, although it could be made
  --  to contain the number of rows affected by statements in a
  --  later version.
  | CommandComplete L.ByteString
  -- |Each DataRow (result of a query) is a list of ByteStrings
  --  (or just Nothing for null values, to distinguish them from
  --  emtpy strings). The ByteStrings can then be converted to
  --  the appropriate type by 'pgStringToType'.
  | DataRow PGData
  | EmptyQueryResponse
  -- |An ErrorResponse contains the severity, "SQLSTATE", and
  --  message of an error. See
  --  <http://www.postgresql.org/docs/current/interactive/protocol-error-fields.html>.
  | ErrorResponse { messageFields :: MessageFields }
  | NoData
  | NoticeResponse { messageFields :: MessageFields }
  -- |A ParameterDescription describes the type of a given SQL
  --  query/statement parameter ($1, $2, etc.). Unfortunately,
  --  PostgreSQL does not give us nullability information for the
  --  parameter.
  | ParameterDescription [OID]
  | ParameterStatus String String
  | ParseComplete
  | PortalSuspended
  | ReadyForQuery PGState
  -- |A RowDescription contains the name, type, table OID, and
  --  column number of the resulting columns(s) of a query. The
  --  column number is useful for inferring nullability.
  | RowDescription [ColDescription]
  deriving (Show)

-- |PGException is thrown upon encountering an 'ErrorResponse' with severity of
--  ERROR, FATAL, or PANIC. It holds the message of the error.
data PGError = PGError MessageFields
  deriving (Typeable)

instance Show PGError where
  show (PGError m) = displayMessage m

instance Exception PGError

-- |Produce a human-readable string representing the message
displayMessage :: MessageFields -> String
displayMessage m = "PG" ++ f 'S' ++ " [" ++ f 'C' ++ "]: " ++ f 'M' ++ '\n' : f 'D'
  where f c = maybe "" U.toString $ Map.lookup (c2w c) m

-- |Message SQLState code.
--  See <http://www.postgresql.org/docs/current/static/errcodes-appendix.html>.
messageCode :: MessageFields -> String
messageCode = maybe "" LC.unpack . Map.lookup (c2w 'C')

defaultLogMessage :: MessageFields -> IO ()
defaultLogMessage = hPutStrLn stderr . displayMessage

#ifdef USE_MD5
md5 :: L.ByteString -> L.ByteString
md5 = L.fromStrict . Hash.digestToHexByteString . (Hash.hashlazy :: L.ByteString -> Hash.Digest Hash.MD5)
#endif


nul :: B.Builder
nul = B.word8 0

-- |Convert a string to a NULL-terminated UTF-8 string. The PostgreSQL
--  protocol transmits most strings in this format.
pgString :: String -> B.Builder
pgString s = B.stringUtf8 s <> nul

-- |Given a message, determinal the (optional) type ID and the body
messageBody :: PGFrontendMessage -> (Maybe Char, B.Builder)
messageBody (StartupMessage kv) = (Nothing, B.word32BE 0x30000
  <> foldMap (\(k, v) -> pgString k <> pgString v) kv <> nul)
messageBody (CancelRequest pid key) = (Nothing, B.word32BE 80877102
  <> B.word32BE pid <> B.word32BE key)
messageBody Bind{ statementName = n, bindParameters = p } = (Just 'B',
  nul <> pgString n
    <> B.word16BE 0
    <> B.word16BE (fromIntegral $ length p) <> foldMap (maybe (B.word32BE 0xFFFFFFFF) val) p
    <> B.word16BE 0)
  where val v = B.word32BE (fromIntegral $ L.length v) <> B.lazyByteString v
messageBody Close{ statementName = n } = (Just 'C', 
  B.char7 'S' <> pgString n)
messageBody Describe{ statementName = n } = (Just 'D',
  B.char7 'S' <> pgString n)
messageBody (Execute r) = (Just 'E',
  nul <> B.word32BE r)
messageBody Flush = (Just 'H', mempty)
messageBody Parse{ statementName = n, queryString = s, parseTypes = t } = (Just 'P',
  pgString n <> pgString s
    <> B.word16BE (fromIntegral $ length t) <> foldMap B.word32BE t)
messageBody (PasswordMessage s) = (Just 'p',
  B.lazyByteString s <> nul)
messageBody SimpleQuery{ queryString = s } = (Just 'Q',
  pgString s)
messageBody Sync = (Just 'S', mempty)
messageBody Terminate = (Just 'X', mempty)

-- |Send a message to PostgreSQL (low-level).
pgSend :: PGConnection -> PGFrontendMessage -> IO ()
pgSend PGConnection{ connHandle = h, connDebug = d, connState = sr } msg = do
  writeIORef sr StateUnknown
  when d $ putStrLn $ "> " ++ show msg
  B.hPutBuilder h $ foldMap B.char7 t <> B.word32BE (fromIntegral $ 4 + L.length b)
  L.hPut h b -- or B.hPutBuilder? But we've already had to convert to BS to get length
  where (t, b) = second B.toLazyByteString $ messageBody msg

pgFlush :: PGConnection -> IO ()
pgFlush = hFlush . connHandle


getPGString :: G.Get String
getPGString = U.toString <$> G.getLazyByteStringNul

getMessageFields :: G.Get MessageFields
getMessageFields = g =<< G.getWord8 where
  g 0 = return Map.empty
  g f = liftM2 (Map.insert f) G.getLazyByteStringNul getMessageFields

-- |Parse an incoming message.
getMessageBody :: Char -> G.Get PGBackendMessage
getMessageBody 'R' = auth =<< G.getWord32be where
  auth 0 = return AuthenticationOk
  auth 3 = return AuthenticationCleartextPassword
  auth 5 = AuthenticationMD5Password <$> G.getLazyByteString 4
  auth op = fail $ "pgGetMessage: unsupported authentication type: " ++ show op
getMessageBody 't' = do
  numParams <- G.getWord16be
  ParameterDescription <$> replicateM (fromIntegral numParams) G.getWord32be
getMessageBody 'T' = do
  numFields <- G.getWord16be
  RowDescription <$> replicateM (fromIntegral numFields) getField where
  getField = do
    name <- getPGString
    oid <- G.getWord32be -- table OID
    col <- G.getWord16be -- column number
    typ' <- G.getWord32be -- type
    _ <- G.getWord16be -- type size
    _ <- G.getWord32be -- type modifier
    0 <- G.getWord16be -- format code
    return $ ColDescription
      { colName = name
      , colTable = oid
      , colNumber = fromIntegral col
      , colType = typ'
      }
getMessageBody 'Z' = ReadyForQuery <$> (rs . w2c =<< G.getWord8) where
  rs 'I' = return StateIdle
  rs 'T' = return StateTransaction
  rs 'E' = return StateTransactionFailed
  rs s = fail $ "pgGetMessage: unknown ready state: " ++ show s
getMessageBody '1' = return ParseComplete
getMessageBody 'C' = CommandComplete <$> G.getLazyByteStringNul
getMessageBody 'S' = liftM2 ParameterStatus getPGString getPGString
getMessageBody 'D' = do 
  numFields <- G.getWord16be
  DataRow <$> replicateM (fromIntegral numFields) (getField =<< G.getWord32be) where
  getField 0xFFFFFFFF = return Nothing
  getField len = Just <$> G.getLazyByteString (fromIntegral len)
getMessageBody 'K' = liftM2 BackendKeyData G.getWord32be G.getWord32be
getMessageBody 'E' = ErrorResponse <$> getMessageFields
getMessageBody 'I' = return EmptyQueryResponse
getMessageBody 'n' = return NoData
getMessageBody 's' = return PortalSuspended
getMessageBody 'N' = NoticeResponse <$> getMessageFields
getMessageBody t = fail $ "pgGetMessage: unknown message type: " ++ show t

runGet :: Monad m => G.Get a -> L.ByteString -> m a
runGet g s = either (\(_, _, e) -> fail e) (\(_, _, r) -> return r) $ G.runGetOrFail g s

-- |Receive the next message from PostgreSQL (low-level). Note that this will
-- block until it gets a message.
pgReceive :: PGConnection -> IO PGBackendMessage
pgReceive c@PGConnection{ connHandle = h, connDebug = d } = do
  (typ, len) <- runGet (liftM2 (,) G.getWord8 G.getWord32be) =<< L.hGet h 5
  msg <- runGet (getMessageBody $ w2c typ) =<< L.hGet h (fromIntegral len - 4)
  when d $ putStrLn $ "< " ++ show msg
  case msg of
    ReadyForQuery s -> msg <$ writeIORef (connState c) s
    NoticeResponse{ messageFields = m } ->
      connLogMessage c m >> pgReceive c
    ErrorResponse{ messageFields = m } ->
      writeIORef (connState c) StateUnknown >> throwIO (PGError m) 
    _ -> return msg

-- |Connect to a PostgreSQL server.
pgConnect :: HostName  -- ^ the host to connect to
          -> PortID    -- ^ the port to connect on
          -> String    -- ^ the database to connect to
          -> String    -- ^ the username to connect as
          -> String    -- ^ the password to connect with
          -> IO PGConnection -- ^ a handle to communicate with the PostgreSQL server on
pgConnect host port db user pass = do
  debug <- isJust <$> lookupEnv "TPG_DEBUG"
  state <- newIORef StateUnknown
  h <- connectTo host port
  let c = PGConnection
        { connHandle = h
        , connDebug = debug
        , connLogMessage = defaultLogMessage
        , connPid = 0
        , connKey = 0
        , connParameters = Map.empty
        , connTypes = defaultTypeMap
        , connState = state
        }
  pgSend c $ StartupMessage
    [ ("user", user)
    , ("database", db)
    , ("client_encoding", "UTF8")
    , ("standard_conforming_strings", "on")
    , ("bytea_output", "hex")
    , ("DateStyle", "ISO, YMD")
    , ("IntervalStyle", "iso_8601")
    ]
  pgFlush c
  conn c
  where
  conn c = msg c =<< pgReceive c
  msg c (ReadyForQuery _) = return c
  msg c (BackendKeyData p k) = conn c{ connPid = p, connKey = k }
  msg c (ParameterStatus k v) = conn c{ connParameters = Map.insert k v $ connParameters c }
  msg c AuthenticationOk = conn c
  msg c AuthenticationCleartextPassword = do
    pgSend c $ PasswordMessage $ U.fromString pass
    pgFlush c
    conn c
#ifdef USE_MD5
  msg c (AuthenticationMD5Password salt) = do
    pgSend c $ PasswordMessage $ LC.pack "md5" `L.append` md5 (md5 (U.fromString (pass ++ user)) `L.append` salt)
    pgFlush c
    conn c
#endif
  msg _ m = fail $ "pgConnect: unexpected response: " ++ show m

-- |Disconnect from a PostgreSQL server. Note that this currently doesn't send
-- a close message.
pgDisconnect :: PGConnection -- ^ a handle from 'pgConnect'
             -> IO ()
pgDisconnect c@PGConnection{ connHandle = h } = do
  pgSend c Terminate
  hClose h

pgSync :: PGConnection -> IO ()
pgSync c@PGConnection{ connState = sr } = do
  s <- readIORef sr
  when (s == StateUnknown) $ do
    pgSend c Sync
    pgFlush c
    _ <- pgReceive c `catch` \(PGError m) -> ErrorResponse m <$ connLogMessage c m
    pgSync c
    
pgAddType :: OID -> PGTypeHandler -> PGConnection -> PGConnection
pgAddType oid th p = p{ connTypes = Map.insert oid th $ connTypes p }

getTypeOID :: PGConnection -> String -> IO (Maybe (OID, OID))
getTypeOID c t = do
  (_, r) <- pgSimpleQuery c ("SELECT oid, typarray FROM pg_catalog.pg_type WHERE typname = " ++ pgLiteral t)
  case r of
    [] -> return Nothing
    [[Just o, Just lo]] -> return (Just (pgDecodeBS o, pgDecodeBS lo))
    _ -> fail $ "Unexpected PostgreSQL type result for " ++ t ++ ": " ++ show r

getPGType :: PGConnection -> OID -> IO PGTypeHandler
getPGType c@PGConnection{ connTypes = types } oid =
  maybe notype return $ Map.lookup oid types where
  notype = do
    (_, r) <- pgSimpleQuery c ("SELECT typname FROM pg_catalog.pg_type WHERE oid = " ++ pgLiteral oid)
    case r of
      [[Just s]] -> fail $ "Unsupported PostgreSQL type " ++ show oid ++ ": " ++ U.toString s
      _ -> fail $ "Unknown PostgreSQL type: " ++ show oid

-- |Describe a SQL statement/query. A statement description consists of 0 or
-- more parameter descriptions (a PostgreSQL type) and zero or more result
-- field descriptions (for queries) (consist of the name of the field, the
-- type of the field, and a nullability indicator).
pgDescribe :: PGConnection -> String -- ^ SQL string
                  -> IO ([PGTypeHandler], [(String, PGTypeHandler, Bool)]) -- ^ a list of parameter types, and a list of result field names, types, and nullability indicators.
pgDescribe h sql = do
  pgSync h
  pgSend h $ Parse{ queryString = sql, statementName = "", parseTypes = [] }
  pgSend h $ Describe ""
  pgSend h $ Flush
  pgFlush h
  ParseComplete <- pgReceive h
  ParameterDescription ps <- pgReceive h
  m <- pgReceive h
  liftM2 (,) (mapM (getPGType h) ps) $ case m of
    NoData -> return []
    RowDescription r -> mapM desc r
    _ -> fail $ "describeStatement: unexpected response: " ++ show m
 where
   desc (ColDescription name tab col typ) = do
     t <- getPGType h typ
     n <- nullable tab col
     return (name, t, n)
   nullable oid col =
     -- We don't get nullability indication from PostgreSQL, at least not
     -- directly.
     if oid == 0
       -- Without any hints, we have to assume that the result can be null and
       -- leave it up to the developer to figure it out.
       then return True
       -- In cases where the resulting field is tracable to the column of a
       -- table, we can check there.
       else do (_, r) <- pgSimpleQuery h ("SELECT attnotnull FROM pg_catalog.pg_attribute WHERE attrelid = " ++ pgLiteral oid ++ " AND attnum = " ++ pgLiteral col)
               case r of
                 [[Just s]] -> return $ not $ pgDecodeBS s
                 [] -> return True
                 _ -> fail $ "Failed to determine nullability of column #" ++ show col

-- |A simple query is one which requires sending only a single 'SimpleQuery'
-- message to the PostgreSQL server. The query is sent as a single string; you
-- cannot bind parameters. Note that queries can return 0 results (an empty
-- list).
pgSimpleQuery :: PGConnection -> String -- ^ SQL string
                   -> IO (Int, [PGData]) -- ^ The number of rows affected and a list of result rows
pgSimpleQuery h sql = do
  pgSync h
  pgSend h $ SimpleQuery sql
  pgFlush h
  go start where 
  go = (>>=) $ pgReceive h
  start (CommandComplete c) = got c
  start (RowDescription _) = go row
  start m = fail $ "executeSimpleQuery: unexpected response: " ++ show m
  row (CommandComplete c) = got c
  row (DataRow fs) = second (fs:) <$> go row
  row m = fail $ "executeSimpleQuery: unexpected row: " ++ show m
  got c = (,) (rowsAffected $ LC.words c) <$> go end
  rowsAffected [] = -1
  rowsAffected l = fromMaybe (-1) $ readMaybe $ LC.unpack $ last l
  end (ReadyForQuery _) = return []
  end EmptyQueryResponse = go end
  end m = fail $ "executeSimpleQuery: unexpected message: " ++ show m
