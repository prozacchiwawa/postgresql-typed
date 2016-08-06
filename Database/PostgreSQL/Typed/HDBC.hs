-- |
-- Module: Database.PostgreSQL.Typed.HDBC
-- Copyright: 2016 Dylan Simon
-- 
-- Use postgresql-typed as a backend for HDBC.
{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
module Database.PostgreSQL.Typed.HDBC
  ( Connection, pgConnection
  , connect
  , reloadTypes
  ) where

import Control.Arrow ((&&&))
import Control.Concurrent.MVar (MVar, newMVar, readMVar, withMVar)
import Control.Exception (handle, throwIO)
import Control.Monad (void, guard)
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy.Char8 as BSLC
import Data.IORef (newIORef, readIORef, writeIORef, modifyIORef')
import Data.Int (Int16)
import qualified Data.IntMap.Lazy as IntMap
import Data.List (uncons)
import qualified Data.Map.Lazy as Map
import Data.Maybe (fromMaybe, isNothing)
import Data.Time.Clock (DiffTime)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import Data.Time.LocalTime (zonedTimeToUTC)
import Data.Word (Word32)
import qualified Database.HDBC.Types as HDBC
import qualified Database.HDBC.ColTypes as HDBC
import System.Mem.Weak (addFinalizer)
import Text.Read (readMaybe)

import Database.PostgreSQL.Typed.Protocol
import Database.PostgreSQL.Typed.Types
import Database.PostgreSQL.Typed.Dynamic
import Database.PostgreSQL.Typed.TH
import Database.PostgreSQL.Typed.SQLToken
import Paths_postgresql_typed (version)

-- |A wrapped 'PGConnection'.
-- This differs from a bare 'PGConnection' in two ways:
--
--   1) It always has exactly one active transaction (with 'pgBegin')
--   2) It automatically disconnects on GC
--
data Connection = Connection
  { pgConnection :: MVar PGConnection -- ^Access the underlying 'PGConnection' directly. You must be careful to ensure that the first invariant is preserved: you should not call 'pgBegin', 'pgCommit', or 'pgRollback' on it. All other operations should be safe.
  , pgServerVer :: String
  , pgTypes :: IntMap.IntMap SqlType
  }

sqlError :: IO a -> IO a
sqlError = handle $ \(PGError m) -> 
  let f c = BSC.unpack $ Map.findWithDefault BSC.empty c m
      fC = f 'C'
      fD = f 'D' in
  throwIO HDBC.SqlError 
    { HDBC.seState = fC
    , HDBC.seNativeError = if null fC then -1 else fromMaybe 0 $ readMaybe (f 'P')
    , HDBC.seErrorMsg = f 'S' ++ ": " ++ f 'M' ++ if null fD then fD else '\n':fD
    }

withPG :: Connection -> (PGConnection -> IO a) -> IO a
withPG c = sqlError . withMVar (pgConnection c)

connect_ :: PGDatabase -> IO PGConnection
connect_ d = sqlError $ do
  pg <- pgConnect d
  addFinalizer pg (pgDisconnectOnce pg)
  pgBegin pg
  return pg

-- |Connect to a database for HDBC use (equivalent to 'pgConnect' and 'pgBegin').
connect :: PGDatabase -> IO Connection
connect d = do
  pg <- connect_ d
  pgv <- newMVar pg
  reloadTypes Connection
    { pgConnection = pgv
    , pgServerVer = maybe "" BSC.unpack $ pgServerVersion pg
    , pgTypes = mempty
    }

-- |Reload the table of all types from the database.
-- This may be needed if you make structural changes to the database.
reloadTypes :: Connection -> IO Connection
reloadTypes c = withPG c $ \pg -> do
  t <- pgLoadTypes pg
  return c{ pgTypes = IntMap.map (sqlType $ pgTypeEnv pg) t }

sqls :: String -> BSLC.ByteString
sqls = BSLC.pack

placeholders :: Int -> [SQLToken] -> [SQLToken]
placeholders n (SQLQMark False : l) = SQLParam n : placeholders (succ n) l
placeholders n (SQLQMark True : l) = SQLQMark False : placeholders n l
placeholders n (t : l) = t : placeholders n l
placeholders _ [] = []

data ColDesc = ColDesc
  { colDescName :: String
  , colDesc :: HDBC.SqlColDesc
  , colDescDecode :: PGValue -> HDBC.SqlValue
  }

data Cursor = Cursor
  { cursorDesc :: [ColDesc]
  , cursorRow :: [PGValues]
  , cursorActive :: Bool
  , _cursorStatement :: HDBC.Statement -- keep a handle to prevent GC
  }

noCursor :: HDBC.Statement -> Cursor
noCursor = Cursor [] [] False

-- |Number of rows to retrieve (and cache) with each call to fetchRow.
-- Ideally this should be configurable, but it's not clear how.
fetchSize :: Word32
fetchSize = 1

getType :: Connection -> PGConnection -> Maybe Bool -> PGColDescription -> ColDesc
getType c pg nul PGColDescription{..} = ColDesc
  { colDescName = BSC.unpack colName
  , colDesc = HDBC.SqlColDesc
    { HDBC.colType = sqlTypeId t
    , HDBC.colSize = fromIntegral colModifier <$ guard (colModifier >= 0)
    , HDBC.colOctetLength = fromIntegral colSize <$ guard (colSize >= 0)
    , HDBC.colDecDigits = Nothing
    , HDBC.colNullable = nul
    }
  , colDescDecode = sqlTypeDecode t
  } where t = IntMap.findWithDefault (sqlType (pgTypeEnv pg) $ show colType) (fromIntegral colType) (pgTypes c)

instance HDBC.IConnection Connection where
  disconnect c = withPG c
    pgDisconnectOnce
  commit c = withPG c $ \pg -> do
    pgCommit pg
    pgBegin pg
  rollback c = withPG c $ \pg -> do
    pgRollback pg
    pgBegin pg
  runRaw c q = withPG c $ \pg ->
    pgSimpleQueries_ pg $ sqls q
  run c q v = withPG c $ \pg -> do
    let q' = sqls $ show $ placeholders 1 $ sqlTokens q
        v' = map encode v
    fromMaybe 0 <$> pgRun pg q' [] v'
  prepare c q = do
    let q' = sqls $ show $ placeholders 1 $ sqlTokens q
    n <- withPG c $ \pg -> pgPrepare pg q' []
    cr <- newIORef $ error "Cursor"
    let
      execute v = withPG c $ \pg -> do
        d <- pgBind pg n (map encode v)
        (r, e) <- pgFetch pg n fetchSize
        modifyIORef' cr $ \p -> p
          { cursorDesc = map (getType c pg Nothing) d
          , cursorRow = r
          , cursorActive = isNothing e
          }
        return $ fromMaybe 0 e
      stmt = HDBC.Statement
        { HDBC.execute = execute
        , HDBC.executeRaw = void $ execute []
        , HDBC.executeMany = mapM_ execute
        , HDBC.finish = withPG c $ \pg -> do
          writeIORef cr $ noCursor stmt
          pgClose pg n
        , HDBC.fetchRow = withPG c $ \pg -> do
          p <- readIORef cr
          fmap (zipWith colDescDecode (cursorDesc p)) <$> case cursorRow p of
            [] | True || cursorActive p -> do
                (rl, e) <- pgFetch pg n fetchSize
                let rl' = uncons rl
                writeIORef cr p
                  { cursorRow = maybe [] snd rl'
                  , cursorActive = isNothing e
                  }
                return $ fst <$> rl'
               | otherwise ->
                return Nothing
            (r:l) -> do
              writeIORef cr p{ cursorRow = l }
              return $ Just r
        , HDBC.getColumnNames =
          map colDescName . cursorDesc <$> readIORef cr
        , HDBC.originalQuery = q
        , HDBC.describeResult =
          map (colDescName &&& colDesc) . cursorDesc <$> readIORef cr
        }
    writeIORef cr $ noCursor stmt
    addFinalizer stmt $ withPG c $ \pg -> pgClose pg n
    return stmt
  clone c = do
    c' <- connect_ . pgConnectionDatabase =<< readMVar (pgConnection c)
    cv <- newMVar c'
    return c{ pgConnection = cv }
  hdbcDriverName _ = "postgresql-typed"
  hdbcClientVer _ = show version
  proxiedClientName = HDBC.hdbcDriverName
  proxiedClientVer = HDBC.hdbcClientVer
  dbServerVer = pgServerVer
  dbTransactionSupport _ = True
  getTables c = withPG c $ \pg ->
    map (pgDecodeRep . head) . snd <$> pgSimpleQuery pg (BSLC.fromChunks
      [ "SELECT relname "
      ,   "FROM pg_class "
      ,   "JOIN pg_namespace "
      ,     "ON relnamespace = pg_namespace.oid "
      ,  "WHERE nspname = ANY (current_schemas(false)) "
      ,    "AND relkind IN ('r','v','m','f')"
      ])
  describeTable c t = withPG c $ \pg -> do
    let makecol ~[attname, attrelid, attnum, atttypid, attlen, atttypmod, attnotnull] =
          colDescName &&& colDesc $ getType c pg (Just $ not $ pgDecodeRep attnotnull) PGColDescription
            { colName = pgDecodeRep attname
            , colTable = pgDecodeRep attrelid
            , colNumber = pgDecodeRep attnum
            , colType = pgDecodeRep atttypid
            , colSize = pgDecodeRep attlen
            , colModifier = pgDecodeRep atttypmod
            , colBinary = False
            }
    map makecol . snd <$> pgSimpleQuery pg (BSLC.fromChunks
      [ "SELECT attname, attrelid, attnum, atttypid, attlen, atttypmod, attnotnull "
      ,   "FROM pg_attribute "
      ,   "JOIN pg_class "
      ,     "ON attrelid = pg_class.oid "
      ,   "JOIN pg_namespace "
      ,     "ON relnamespace = pg_namespace.oid "
      ,  "WHERE nspname = ANY (current_schemas(false)) "
      ,    "AND relkind IN ('r','v','m','f') "
      ,    "AND relname = ", pgLiteralRep t
      ,   " AND attnum > 0 AND NOT attisdropped "
      ,    "ORDER BY attnum"
      ])

encodeRep :: (PGParameter t a, PGRep t a) => a -> PGValue
encodeRep x = PGTextValue $ pgEncode (pgTypeOf x) x

encode :: HDBC.SqlValue -> PGValue
encode (HDBC.SqlString x)                 = encodeRep x
encode (HDBC.SqlByteString x)             = encodeRep x
encode (HDBC.SqlWord32 x)                 = encodeRep x
encode (HDBC.SqlWord64 x)                 = encodeRep (fromIntegral x :: Rational)
encode (HDBC.SqlInt32 x)                  = encodeRep x
encode (HDBC.SqlInt64 x)                  = encodeRep x
encode (HDBC.SqlInteger x)                = encodeRep (fromInteger x :: Rational)
encode (HDBC.SqlChar x)                   = encodeRep x
encode (HDBC.SqlBool x)                   = encodeRep x
encode (HDBC.SqlDouble x)                 = encodeRep x
encode (HDBC.SqlRational x)               = encodeRep x
encode (HDBC.SqlLocalDate x)              = encodeRep x
encode (HDBC.SqlLocalTimeOfDay x)         = encodeRep x
encode (HDBC.SqlZonedLocalTimeOfDay t z)  = encodeRep (t, z)
encode (HDBC.SqlLocalTime x)              = encodeRep x
encode (HDBC.SqlZonedTime x)              = encodeRep (zonedTimeToUTC x)
encode (HDBC.SqlUTCTime x)                = encodeRep x
encode (HDBC.SqlDiffTime x)               = encodeRep (realToFrac x :: DiffTime)
encode (HDBC.SqlPOSIXTime x)              = encodeRep (realToFrac x :: Rational) -- (posixSecondsToUTCTime x)
encode (HDBC.SqlEpochTime x)              = encodeRep (posixSecondsToUTCTime (fromInteger x))
encode (HDBC.SqlTimeDiff x)               = encodeRep (fromIntegral x :: DiffTime)
encode HDBC.SqlNull = PGNullValue

data SqlType = SqlType
  { sqlTypeId :: HDBC.SqlTypeId
  , sqlTypeDecode :: PGValue -> HDBC.SqlValue
  }

sqlType :: PGTypeEnv -> String -> SqlType
sqlType e t = SqlType
  { sqlTypeId = typeId t
  , sqlTypeDecode = decode t e
  }

typeId :: String -> HDBC.SqlTypeId
typeId "boolean"                      = HDBC.SqlBitT
typeId "bytea"                        = HDBC.SqlVarBinaryT
typeId "\"char\""                     = HDBC.SqlCharT
typeId "name"                         = HDBC.SqlVarCharT
typeId "bigint"                       = HDBC.SqlBigIntT
typeId "smallint"                     = HDBC.SqlSmallIntT
typeId "integer"                      = HDBC.SqlIntegerT
typeId "text"                         = HDBC.SqlLongVarCharT
typeId "oid"                          = HDBC.SqlIntegerT
typeId "real"                         = HDBC.SqlFloatT
typeId "double precision"             = HDBC.SqlDoubleT
typeId "abstime"                      = HDBC.SqlUTCDateTimeT
typeId "reltime"                      = HDBC.SqlIntervalT HDBC.SqlIntervalSecondT
typeId "tinterval"                    = HDBC.SqlIntervalT HDBC.SqlIntervalDayToSecondT
typeId "bpchar"                       = HDBC.SqlVarCharT
typeId "character varying"            = HDBC.SqlVarCharT
typeId "date"                         = HDBC.SqlDateT
typeId "time without time zone"       = HDBC.SqlTimeT
typeId "timestamp without time zone"  = HDBC.SqlTimestampT
typeId "timestamp with time zone"     = HDBC.SqlTimestampWithZoneT -- XXX really SQLUTCDateTimeT
typeId "interval"                     = HDBC.SqlIntervalT HDBC.SqlIntervalDayToSecondT
typeId "time with time zone"          = HDBC.SqlTimeWithZoneT
typeId "numeric"                      = HDBC.SqlDecimalT
typeId "uuid"                         = HDBC.SqlGUIDT
typeId t = HDBC.SqlUnknownT t

decodeRep :: PGColumn t a => PGTypeName t -> PGTypeEnv -> (a -> HDBC.SqlValue) -> PGValue -> HDBC.SqlValue
decodeRep t e f (PGBinaryValue v) = f $ pgDecodeBinary e t v
decodeRep t _ f (PGTextValue v) = f $ pgDecode t v
decodeRep _ _ _ PGNullValue = HDBC.SqlNull

#define DECODE(T) \
  decode T e = decodeRep (PGTypeProxy :: PGTypeName T) e

decode :: String -> PGTypeEnv -> PGValue -> HDBC.SqlValue
DECODE("boolean")                     HDBC.SqlBool
DECODE("\"char\"")                    HDBC.SqlChar
DECODE("name")                        HDBC.SqlString
DECODE("bigint")                      HDBC.SqlInt64
DECODE("smallint")                    (HDBC.SqlInt32 . fromIntegral :: Int16 -> HDBC.SqlValue)
DECODE("integer")                     HDBC.SqlInt32
DECODE("text")                        HDBC.SqlString
DECODE("oid")                         HDBC.SqlWord32
DECODE("real")                        HDBC.SqlDouble
DECODE("double precision")            HDBC.SqlDouble
DECODE("bpchar")                      HDBC.SqlString
DECODE("character varying")           HDBC.SqlString
DECODE("date")                        HDBC.SqlLocalDate
DECODE("time without time zone")      HDBC.SqlLocalTimeOfDay
DECODE("time with time zone")         (uncurry HDBC.SqlZonedLocalTimeOfDay)
DECODE("timestamp without time zone") HDBC.SqlLocalTime
DECODE("timestamp with time zone")    HDBC.SqlUTCTime
DECODE("interval")                    (HDBC.SqlDiffTime . realToFrac :: DiffTime -> HDBC.SqlValue)
DECODE("numeric")                     HDBC.SqlRational
decode _ _ = decodeRaw where
  decodeRaw (PGBinaryValue v) = HDBC.SqlByteString v
  decodeRaw (PGTextValue v)   = HDBC.SqlByteString v
  decodeRaw PGNullValue       = HDBC.SqlNull
