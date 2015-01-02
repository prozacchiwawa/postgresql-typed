{-# LANGUAGE PatternGuards, ScopedTypeVariables, FlexibleContexts #-}
-- |
-- Module: Database.TemplatePG.TH
-- Copyright: 2015 Dylan Simon
-- 
-- Support functions for compile-time PostgreSQL connection and state management.
-- Although this is meant to be used from other TH code, it will work during normal runtime if just want simple PGConnection management.

module Database.TemplatePG.TH
  ( getTPGDatabase
  , withTPGState
  , withTPGConnection
  , useTPGDatabase
  , registerTPGType
  , getTPGTypeOID
  , getTPGType
  , tpgDescribe
  , pgTypeDecoder
  , pgTypeDecoderNotNull
  , pgTypeEncoder
  , pgTypeEscaper
  ) where

import Control.Applicative ((<$>), (<$), (<|>))
import Control.Concurrent.MVar (MVar, newMVar, modifyMVar, modifyMVar_, swapMVar)
import Control.Monad ((>=>), void, liftM2)
import Data.Foldable (toList)
import Data.List (find)
import qualified Data.Map as Map
import Data.Maybe (isJust, fromMaybe)
import qualified Language.Haskell.TH as TH
import Network (PortID(UnixSocket, PortNumber), PortNumber)
import System.Environment (lookupEnv)
import System.IO.Unsafe (unsafePerformIO)
import Text.Read (readMaybe)

import Database.TemplatePG.Types
import Database.TemplatePG.Protocol

data TPGState = TPGState
  { tpgConnection :: PGConnection
  , tpgTypes :: PGTypeMap
  }

-- |Generate a 'PGDatabase' based on the environment variables:
-- @TPG_HOST@ (localhost); @TPG_SOCK@ or @TPG_PORT@ (5432); @TPG_DB@ or user; @TPG_USER@ or @USER@ (postgres); @TPG_PASS@ ()
getTPGDatabase :: IO PGDatabase
getTPGDatabase = do
  user <- fromMaybe "postgres" <$> liftM2 (<|>) (lookupEnv "TPG_USER") (lookupEnv "USER")
  db   <- fromMaybe user <$> lookupEnv "TPG_DB"
  host <- fromMaybe "localhost" <$> lookupEnv "TPG_HOST"
  pnum <- maybe (5432 :: PortNumber) ((fromIntegral :: Int -> PortNumber) . read) <$> lookupEnv "TPG_PORT"
  port <- maybe (PortNumber pnum) UnixSocket <$> lookupEnv "TPG_SOCK"
  pass <- fromMaybe "" <$> lookupEnv "TPG_PASS"
  debug <- isJust <$> lookupEnv "TPG_DEBUG"
  return $ defaultPGDatabase
    { pgDBHost = host
    , pgDBPort = port
    , pgDBName = db
    , pgDBUser = user
    , pgDBPass = pass
    , pgDBDebug = debug
    }

tpgConnect :: PGDatabase -> IO TPGState
tpgConnect db = do
  c <- pgConnect db
  return $ TPGState c defaultPGTypeMap

tpgState :: MVar (Either (IO TPGState) TPGState)
tpgState = unsafePerformIO $ newMVar $ Left $ tpgConnect =<< getTPGDatabase

withTPGState :: (TPGState -> IO a) -> IO a
withTPGState f = modifyMVar tpgState $ either id return >=> (\c -> (,) (Right c) <$> f c)

-- |Run an action using the TemplatePG connection.
withTPGConnection :: (PGConnection -> IO a) -> IO a
withTPGConnection f = withTPGState (f . tpgConnection)

setTPGState :: Either (IO TPGState) TPGState -> IO ()
setTPGState = void . swapMVar tpgState

-- |Specify an alternative database to use during TemplatePG compilation.
-- This lets you override the default connection parameters that are based on TPG environment variables.
-- This should be called as a top-level declaration and produces no code.
-- It will also clear all types registered with 'registerTPGType'.
useTPGDatabase :: PGDatabase -> TH.Q [TH.Dec]
useTPGDatabase db = [] <$ TH.runIO (setTPGState $ Left $ tpgConnect db)

modifyTPGState :: (TPGState -> TPGState) -> IO ()
modifyTPGState f = modifyMVar_ tpgState $ return . either (Left . fmap f) (Right . f)

-- |Add a new type handler for the given type OID.
tpgAddType :: PGTypeHandler -> TPGState -> TPGState
tpgAddType h tpg = tpg{ tpgTypes = Map.insert (pgTypeOID h) h $ tpgTypes tpg }

-- |Lookup the OID of a database type by internal or formatted name (case sensitive).
-- Fail if not found.
getTPGTypeOID :: TPGState -> String -> IO (OID, OID)
getTPGTypeOID TPGState{ tpgConnection = c, tpgTypes = types } t
  | Just oid <- readMaybe t = return (oid, 0)
  | Just oid <- findType t = return (oid, fromMaybe 0 $ findType ('_':t)) -- optimization, sort of
  | otherwise = do
    (_, r) <- pgSimpleQuery c ("SELECT oid, typarray FROM pg_catalog.pg_type WHERE typname = " ++ pgQuote t ++ " OR format_type(oid, -1) = " ++ pgQuote t)
    case toList r of
      [] -> fail $ "Unknown PostgreSQL type: " ++ t
      [[Just o, Just lo]] -> return (decodeOID o, decodeOID lo)
      _ -> fail $ "Unexpected PostgreSQL type result for " ++ t ++ ": " ++ show r
  where
  findType n = fmap fst $ find ((==) n . pgTypeName' . snd) $ Map.toList types
  decodeOID = pgDecode pgOIDType

-- |Lookup the type handler for a given type OID.
getTPGType :: TPGState -> OID -> IO PGTypeHandler
getTPGType TPGState{ tpgConnection = c, tpgTypes = types } oid =
  maybe notype return $ Map.lookup oid types where
  notype = do
    (_, r) <- pgSimpleQuery c ("SELECT typname FROM pg_catalog.pg_type WHERE oid = " ++ pgLiteral pgOIDType oid)
    case toList r of
      [[Just s]] -> fail $ "Unsupported PostgreSQL type " ++ show oid ++ ": " ++ show s
      _ -> fail $ "Unknown PostgreSQL type: " ++ show oid

-- |Register a new handler for PostgreSQL type and a Haskell type, which should be an instance of 'PGType'.
-- This should be called as a top-level declaration and produces no code.
registerTPGType :: String -> TH.Type -> TH.Q [TH.Dec]
registerTPGType name typ = TH.runIO $ do
  (oid, loid) <- withTPGState (\c -> getTPGTypeOID c name)
  modifyTPGState (
    (if loid == 0 then id else tpgAddType (pgArrayType loid name typ))
    . tpgAddType (PGTypeHandler oid name typ))
  return []

-- |A type-aware wrapper to 'pgDescribe'
tpgDescribe :: TPGState -> String -> [String] -> Bool -> IO ([PGTypeHandler], [(String, PGTypeHandler, Bool)])
tpgDescribe tpg sql types nulls = do
  at <- mapM (fmap fst . getTPGTypeOID tpg) types
  (pt, rt) <- pgDescribe (tpgConnection tpg) sql at nulls
  pth <- mapM (getTPGType tpg) pt
  rth <- mapM (\(c, t, n) -> do
    th <- getTPGType tpg t
    return (c, th, n)) rt
  return (pth, rth)


typeApply :: TH.Name -> String -> TH.Exp
typeApply f s = TH.AppE (TH.VarE f) $
  TH.ConE 'PGTypeProxy `TH.SigE` (TH.ConT ''PGTypeName `TH.AppT` TH.LitT (TH.StrTyLit s))


-- |TH expression to decode a 'Maybe' 'L.ByteString' to a 'Maybe' 'PGColumn' value.
pgTypeDecoder :: String -> TH.Exp
pgTypeDecoder = typeApply 'pgDecodeColumn

-- |TH expression to decode a 'Maybe' 'L.ByteString' to a 'PGColumn' value.
pgTypeDecoderNotNull :: String -> TH.Exp
pgTypeDecoderNotNull = typeApply 'pgDecodeColumnNotNull

-- |TH expression to encode a 'PGParameter' value to a 'Maybe' 'L.ByteString'.
pgTypeEncoder :: String -> TH.Exp
pgTypeEncoder = typeApply 'pgEncodeParameter

-- |TH expression to escape a 'PGParameter' value to a SQL literal.
pgTypeEscaper :: String -> TH.Exp
pgTypeEscaper = typeApply 'pgEscapeParameter

