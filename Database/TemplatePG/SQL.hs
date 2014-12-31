-- Copyright 2010, 2011, 2012, 2013 Chris Forno

-- |This module exposes the high-level Template Haskell interface for querying
-- and manipulating the PostgreSQL server.
-- 
-- All SQL string arguments support expression interpolation. Just enclose your
-- expression in @{}@ in the SQL string.
-- 
-- Note that transactions are messy and untested. Attempt to use them at your
-- own risk.

module Database.TemplatePG.SQL ( queryTuples
                               , queryTuple
                               , execute
                               , insertIgnore
                               , withTransaction
                               , rollback
                               ) where

import Control.Exception (onException, catchJust)
import Control.Monad (liftM, void, guard)
import Data.Maybe (listToMaybe)
import Language.Haskell.TH

import Database.TemplatePG.Protocol
import Database.TemplatePG.Query

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
-- @$(queryTuples \"SELECT usesysid, usename FROM pg_user\") h :: IO [(Maybe String, Maybe Integer)]
-- @
queryTuples :: String -> Q Exp
queryTuples sql = [| \c -> pgQuery c $(makePGSimpleQuery $ querySQL sql) |]

-- |@queryTuple :: String -> (PGConnection -> IO (Maybe (column1, column2, ...)))@
-- 
-- Convenience function to query a PostgreSQL server and return the first
-- result as a tuple. If the query produces no results, return 'Nothing'.
-- 
-- Example (where @h@ is a handle from 'pgConnect'):
-- 
-- @let sysid = 10::Integer;
-- 
-- $(queryTuple \"SELECT usesysid, usename FROM pg_user WHERE usesysid = {sysid}\") h :: IO (Maybe (Maybe String, Maybe Integer))
-- @
queryTuple :: String -> Q Exp
queryTuple sql = [| liftM listToMaybe . $(queryTuples sql) |]

-- |@execute :: String -> (PGConnection -> IO ())@
-- 
-- Convenience function to execute a statement on the PostgreSQL server.
-- 
-- Example (where @h@ is a handle from 'pgConnect'):
-- 
-- @let rolename = \"BOfH\"
-- 
-- $(execute \"CREATE ROLE {rolename}\") h
-- @
execute :: String -> Q Exp
execute sql = [| \c -> void $ pgExecute c $(makePGSimpleQuery $ querySQL sql) |]

-- |Run a sequence of IO actions (presumably SQL statements) wrapped in a
-- transaction. Unfortunately you're restricted to using this in the 'IO'
-- Monad for now due to the use of 'onException'. I'm debating adding a
-- 'MonadPeelIO' version.
withTransaction :: PGConnection -> IO a -> IO a
withTransaction h a =
  onException (do void $ pgSimpleQuery h "BEGIN"
                  c <- a
                  void $ pgSimpleQuery h "COMMIT"
                  return c)
              (void $ pgSimpleQuery h "ROLLBACK")

-- |Roll back a transaction.
rollback :: PGConnection -> IO ()
rollback h = void $ pgSimpleQuery h "ROLLBACK"

-- |Ignore duplicate key errors. This is also limited to the 'IO' Monad.
insertIgnore :: IO () -> IO ()
insertIgnore q = catchJust uniquenessError q (\ _ -> return ()) where
  uniquenessError (PGError m) = guard (messageCode m == "24505")
