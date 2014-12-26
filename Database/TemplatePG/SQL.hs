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
                               , withTHConnection
                               ) where

import Database.TemplatePG.Protocol
import Database.TemplatePG.Types

import Control.Applicative ((<$>))
import Control.Concurrent.MVar (MVar, newMVar, modifyMVar)
import Control.Exception (onException, catchJust)
import Control.Monad (zipWithM, liftM)
import Data.ByteString.Lazy.UTF8 hiding (length, decode, take, foldr)
import Data.Maybe (fromMaybe, fromJust)
import Language.Haskell.Meta.Parse (parseExp)
import Language.Haskell.TH
import Language.Haskell.TH.Syntax (returnQ)
import Network (PortID(UnixSocket, PortNumber), PortNumber)
import System.Environment (getEnv, lookupEnv)
import System.IO.Unsafe (unsafePerformIO)
import qualified Text.ParserCombinators.Parsec as P

-- |Grab a PostgreSQL connection for compile time. We do so through the
-- environment variables: @TPG_DB@, @TPG_HOST@, @TPG_PORT@, @TPG_USER@, and
-- @TPG_PASS@. Only TPG_DB is required.
thConnection :: MVar (IO PGConnection)
thConnection = unsafePerformIO $ newMVar $ do
  database <- getEnv "TPG_DB"
  hostName <- fromMaybe "localhost" <$> lookupEnv "TPG_HOST"
  socket   <- lookupEnv "TPG_SOCK"
  portNum  <- maybe (5432 :: PortNumber) (fromIntegral . read) <$> lookupEnv "TPG_PORT"
  username <- fromMaybe "postgres" <$> lookupEnv "TPG_USER"
  password <- fromMaybe "" <$> lookupEnv "TPG_PASS"
  let portId = maybe (PortNumber $ fromIntegral portNum) UnixSocket socket
  pgConnect hostName portId database username password

withTHConnection :: (PGConnection -> IO a) -> IO a
withTHConnection f = modifyMVar thConnection $ (=<<) $ \c -> (,) (return c) <$> f c

-- |This is where most of the magic happens.
-- This doesn't result in a PostgreSQL prepared statement, it just creates one
-- to do type inference.
-- This returns a prepared SQL string with all values (as an expression)
prepareSQL :: String -- ^ a SQL string, with
           -> Q (Exp, [(String, PGType, Bool)]) -- ^ a prepared SQL string and result descriptions
prepareSQL sql = do
  (pTypes, fTypes) <- runIO $ withTHConnection $ \c ->
    describeStatement c (holdPlaces sqlStrings expStrings)
  s <- weaveString sqlStrings =<< zipWithM stringify pTypes expStrings
  return (s, fTypes)
 where holdPlaces ss es = concat $ weave ss (take (length es) placeholders)
       placeholders = map (('$' :) . show) ([1..]::[Integer])
       stringify typ s = [| $(pgTypeToString typ) $(returnQ $ parseExp' s) |]
       parseExp' e = (either (\ _ -> error ("Failed to parse expression: " ++ e)) id) $ parseExp e
       (sqlStrings, expStrings) = parseSql sql

-- |"weave" 2 lists of equal length into a single list.
weave :: [a] -> [a] -> [a]
weave x []          = x
weave [] y          = y
weave (x:xs) (y:ys) = x:y:(weave xs ys)

-- |"weave" a list of SQL fragements an Haskell expressions into a single SQL string.
weaveString :: [String] -- ^ SQL fragments
            -> [Exp]    -- ^ Haskell expressions
            -> Q Exp
weaveString [x]    []     = [| x |]
weaveString []     [y]    = returnQ y
weaveString (x:[]) (y:[]) = [| x ++ $(returnQ y) |]
weaveString (x:xs) (y:ys) = [| x ++ $(returnQ y) ++ $(weaveString xs ys) |]
weaveString _      _      = error "Weave mismatch (possible parse problem)"

-- |@queryTuples :: String -> (PGConnection -> IO [(column1, column2, ...)])@
-- 
-- Query a PostgreSQL server and return the results as a list of tuples.
-- 
-- Example (where @h@ is a handle from 'pgConnect'):
-- 
-- @$(queryTuples \"SELECT usesysid, usename FROM pg_user\") h
-- 
-- => IO [(Maybe String, Maybe Integer)]
-- @
queryTuples :: String -> Q Exp
queryTuples sql = do
  (sql', types) <- prepareSQL sql
  [| liftM (map $(convertRow types)) . executeSimpleQuery $(returnQ sql') |]

-- |@queryTuple :: String -> (PGConnection -> IO (Maybe (column1, column2, ...)))@
-- 
-- Convenience function to query a PostgreSQL server and return the first
-- result as a tuple. If the query produces no results, return 'Nothing'.
-- 
-- Example (where @h@ is a handle from 'pgConnect'):
-- 
-- @let sysid = 10::Integer;
-- 
-- $(queryTuple \"SELECT usesysid, usename FROM pg_user WHERE usesysid = {sysid}\") h
-- 
-- => IO (Maybe (Maybe String, Maybe Integer))
-- @
queryTuple :: String -> Q Exp
queryTuple sql = [| liftM maybeHead . $(queryTuples sql) |]

maybeHead :: [a] -> Maybe a
maybeHead []    = Nothing
maybeHead (x:_) = Just x

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
execute sql = do
  (sql', types) <- prepareSQL sql
  case types of
    [] -> [| executeSimpleStatement $(returnQ sql') |]
    _  -> error "Execute can't be used on queries, only statements."

-- |Run a sequence of IO actions (presumably SQL statements) wrapped in a
-- transaction. Unfortunately you're restricted to using this in the 'IO'
-- Monad for now due to the use of 'onException'. I'm debating adding a
-- 'MonadPeelIO' version.
withTransaction :: PGConnection -> IO a -> IO a
withTransaction h a =
  onException (do executeSimpleStatement "BEGIN" h
                  c <- a
                  executeSimpleStatement "COMMIT" h
                  return c)
              (executeSimpleStatement "ROLLBACK" h)

-- |Roll back a transaction.
rollback :: PGConnection -> IO ()
rollback = executeSimpleStatement "ROLLBACK"

-- |Ignore duplicate key errors. This is also limited to the 'IO' Monad.
insertIgnore :: IO () -> IO ()
insertIgnore q = catchJust uniquenessError q (\ _ -> return ())
 where uniquenessError e = case e of
                             PGException m -> case messageCode m of
                                                    "23505" -> Just ()
                                                    _       -> Nothing

-- |Given a result description, create a function to convert a result to a
-- tuple.
convertRow :: [(String, PGType, Bool)] -- ^ result description
           -> Q Exp -- ^ A function for converting a row of the given result description
convertRow types = do
  n <- newName "result"
  lamE [varP n] $ tupE $ map (convertColumn n) $ zip types [0..]

-- |Given a raw PostgreSQL result and a result field type, convert the
-- appropriate field to a Haskell value.
convertColumn :: Name  -- ^ the name of the variable containing the result list (of 'Maybe' 'ByteString')
              -> ((String, PGType, Bool), Int) -- ^ the result field type and index
              -> Q Exp
convertColumn name ((_, typ, nullable), i) = [| $(pgStringToType' typ nullable) ($(varE name) !! i) |]

-- |Like 'pgStringToType', but deal with possible @NULL@s. If the boolean
-- argument is 'False', that means that we know that the value is not nullable
-- and we can use 'fromJust' to keep the code simple. If it's 'True', then we
-- don't know if the value is nullable and must return a 'Maybe' value in case
-- it is.
pgStringToType' :: PGType
                -> Bool  -- ^ nullability indicator
                -> Q Exp
pgStringToType' t False = [| ($(pgStringToType t)) . toString . fromJust |]
pgStringToType' t True  = [| liftM (($(pgStringToType t)) . toString) |]

-- SQL Parser --

-- |Given a SQL string return a list of SQL parts and expression parts.
-- For example: @\"SELECT * FROM table WHERE id = {someID} AND age > {baseAge * 1.5}\"@
-- becomes: @(["SELECT * FROM table WHERE id = ", " AND age > "],
--            ["someID", "baseAge * 1.5"])@
parseSql :: String -> ([String], [String])
parseSql sql = case (P.parse sqlStatement "" sql) of
                 Left err -> error (show err)
                 Right ss -> every2nd ss

every2nd :: [a] -> ([a], [a])
every2nd = foldr (\a ~(x,y) -> (a:y,x)) ([],[])

sqlStatement :: P.Parser [String]
sqlStatement = P.many1 $ P.choice [sqlText, sqlParameter]

sqlText :: P.Parser String
sqlText = P.many1 (P.noneOf "{")

-- |Parameters are enclosed in @{}@ and can be any Haskell expression supported
-- by haskell-src-meta.
sqlParameter :: P.Parser String
sqlParameter = P.between (P.char '{') (P.char '}') $ P.many1 (P.noneOf "}")
