{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -Wno-deprecations #-}
-- Copyright 2010, 2011, 2012, 2013 Chris Forno

-- |This module exposes the high-level Template Haskell interface for querying
-- and manipulating the PostgreSQL server.
-- 
-- All SQL string arguments support expression interpolation. Just enclose your
-- expression in @{}@ in the SQL string.
-- 
-- Note that transactions are messy and untested. Attempt to use them at your
-- own risk.

module Database.PostgreSQL.Typed.TemplatePG 
  ( queryTuples
  , queryTuple
  , execute
  , insertIgnore
  , withTransaction
  , rollback
  , PGException
  , pgConnect
  , PG.pgDisconnect
  ) where

import           Control.Exception (catchJust)
import           Control.Monad (liftM, void, guard)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy.Char8 as BSLC
import           Data.Maybe (listToMaybe, isJust)
import qualified Language.Haskell.TH as TH
import           Network (HostName, PortID(..))
import qualified Network.Socket as Net
import           System.Environment (lookupEnv)

import qualified Database.PostgreSQL.Typed.Protocol as PG
import Database.PostgreSQL.Typed.Query

-- |Convert a 'queryTuple'-style string with placeholders into a new style SQL string.
querySQL :: String -> String
querySQL ('{':s) = '$':'{':querySQL s
querySQL (c:s) = c:querySQL s
querySQL "" = ""

-- |@queryTuples :: String -> (PGConnection -> IO [(column1, column2, ...)])@
-- 
-- Query a PostgreSQL server and return the results as a list of tuples.
-- 
-- Example (where @h@ is a handle from 'pgConnect'):
-- 
-- > $(queryTuples "SELECT usesysid, usename FROM pg_user") h :: IO [(Maybe String, Maybe Integer)]
queryTuples :: String -> TH.ExpQ
queryTuples sql = [| \c -> pgQuery c $(makePGQuery simpleQueryFlags $ querySQL sql) |]

-- |@queryTuple :: String -> (PGConnection -> IO (Maybe (column1, column2, ...)))@
-- 
-- Convenience function to query a PostgreSQL server and return the first
-- result as a tuple. If the query produces no results, return 'Nothing'.
-- 
-- Example (where @h@ is a handle from 'pgConnect'):
-- 
-- > let sysid = 10::Integer;
-- > $(queryTuple "SELECT usesysid, usename FROM pg_user WHERE usesysid = {sysid}") h :: IO (Maybe (Maybe String, Maybe Integer))
queryTuple :: String -> TH.ExpQ
queryTuple sql = [| liftM listToMaybe . $(queryTuples sql) |]

-- |@execute :: String -> (PGConnection -> IO ())@
-- 
-- Convenience function to execute a statement on the PostgreSQL server.
-- 
-- Example (where @h@ is a handle from 'pgConnect'):
execute :: String -> TH.ExpQ
execute sql = [| \c -> void $ pgExecute c $(makePGQuery simpleQueryFlags $ querySQL sql) |]

-- |Run a sequence of IO actions (presumably SQL statements) wrapped in a
-- transaction. Unfortunately you're restricted to using this in the 'IO'
-- Monad for now due to the use of 'onException'. I'm debating adding a
-- 'MonadPeelIO' version.
withTransaction :: PG.PGConnection -> IO a -> IO a
withTransaction = PG.pgTransaction

-- |Roll back a transaction.
rollback :: PG.PGConnection -> IO ()
rollback h = void $ PG.pgSimpleQuery h $ BSLC.pack "ROLLBACK"

-- |Ignore duplicate key errors. This is also limited to the 'IO' Monad.
insertIgnore :: IO () -> IO ()
insertIgnore q = catchJust uniquenessError q (\ _ -> return ()) where
  uniquenessError e = guard (PG.pgErrorCode e == BSC.pack "23505")

type PGException = PG.PGError

pgConnect :: HostName   -- ^ the host to connect to
          -> PortID     -- ^ the port to connect on
          -> ByteString -- ^ the database to connect to
          -> ByteString -- ^ the username to connect as
          -> ByteString -- ^ the password to connect with
          -> IO PG.PGConnection -- ^ a handle to communicate with the PostgreSQL server on
pgConnect h n d u p = do
  debug <- isJust `liftM` lookupEnv "TPG_DEBUG"
  PG.pgConnect $ PG.defaultPGDatabase
    { PG.pgDBAddr = case n of
        PortNumber s -> Left (h, show s)
        Service    s -> Left (h, s)
        UnixSocket s -> Right (Net.SockAddrUnix s)
    , PG.pgDBName = d
    , PG.pgDBUser = u
    , PG.pgDBPass = p
    , PG.pgDBDebug = debug
    }
