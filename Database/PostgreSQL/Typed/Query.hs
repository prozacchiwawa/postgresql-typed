{-# LANGUAGE CPP, PatternGuards, MultiParamTypeClasses, FunctionalDependencies, TypeSynonymInstances, FlexibleInstances, FlexibleContexts, TemplateHaskell #-}
module Database.PostgreSQL.Typed.Query
  ( PGQuery(..)
  , PGSimpleQuery
  , PGPreparedQuery
  , rawPGSimpleQuery
  , rawPGPreparedQuery
  , QueryFlags(..)
  , simpleQueryFlags
  , parseQueryFlags
  , makePGQuery
  , pgSQL
  , pgExecute
  , pgQuery
  , pgLazyQuery
  ) where

import Control.Applicative ((<$>))
import Control.Arrow ((***), first, second)
import Control.Exception (try)
import Control.Monad (when, mapAndUnzipM)
import Data.Array (listArray, (!), inRange)
import Data.Char (isDigit, isSpace)
import qualified Data.Foldable as Fold
import Data.List (dropWhileEnd)
import Data.Maybe (fromMaybe, isNothing)
import Data.Word (Word32)
import Language.Haskell.Meta.Parse (parseExp)
import qualified Language.Haskell.TH as TH
import Language.Haskell.TH.Quote (QuasiQuoter(..))
import Numeric (readDec)

import Database.PostgreSQL.Typed.Internal
import Database.PostgreSQL.Typed.Types
import Database.PostgreSQL.Typed.Dynamic
import Database.PostgreSQL.Typed.Protocol
import Database.PostgreSQL.Typed.TH

class PGQuery q a | q -> a where
  -- |Execute a query and return the number of rows affected (or -1 if not known) and a list of results.
  pgRunQuery :: PGConnection -> q -> IO (Int, [a])
class PGQuery q PGValues => PGRawQuery q

-- |Execute a query that does not return results.
-- Return the number of rows affected (or -1 if not known).
pgExecute :: PGQuery q () => PGConnection -> q -> IO Int
pgExecute c q = fst <$> pgRunQuery c q

-- |Run a query and return a list of row results.
pgQuery :: PGQuery q a => PGConnection -> q -> IO [a]
pgQuery c q = snd <$> pgRunQuery c q


data SimpleQuery = SimpleQuery String
instance PGQuery SimpleQuery PGValues where
  pgRunQuery c (SimpleQuery sql) = pgSimpleQuery c sql
instance PGRawQuery SimpleQuery where


data PreparedQuery = PreparedQuery String [OID] PGValues [Bool]
instance PGQuery PreparedQuery PGValues where
  pgRunQuery c (PreparedQuery sql types bind bc) = pgPreparedQuery c sql types bind bc
instance PGRawQuery PreparedQuery where


data QueryParser q a = QueryParser (PGTypeEnv -> q) (PGTypeEnv -> PGValues -> a)
instance PGRawQuery q => PGQuery (QueryParser q a) a where
  pgRunQuery c (QueryParser q p) = second (fmap $ p e) <$> pgRunQuery c (q e) where e = pgTypeEnv c

instance Functor (QueryParser q) where
  fmap f (QueryParser q p) = QueryParser q (\e -> f . p e)

rawParser :: q -> QueryParser q PGValues
rawParser q = QueryParser (const q) (const id)

-- |A simple one-shot query that simply substitutes literal representations of parameters for placeholders.
type PGSimpleQuery = QueryParser SimpleQuery
-- |A prepared query that automatically is prepared in the database the first time it is run and bound with new parameters each subsequent time.
type PGPreparedQuery = QueryParser PreparedQuery

-- |Make a simple query directly from a query string, with no type inference
rawPGSimpleQuery :: String -> PGSimpleQuery PGValues
rawPGSimpleQuery = rawParser . SimpleQuery

-- |Make a prepared query directly from a query string and bind parameters, with no type inference
rawPGPreparedQuery :: String -> PGValues -> PGPreparedQuery PGValues
rawPGPreparedQuery sql bind = rawParser $ PreparedQuery sql [] bind []

-- |Run a prepared query in lazy mode, where only chunk size rows are requested at a time.
-- If you eventually retrieve all the rows this way, it will be far less efficient than using @pgQuery@, since every chunk requires an additional round-trip.
-- Although you may safely stop consuming rows early, currently you may not interleave any other database operation while reading rows.  (This limitation could theoretically be lifted if required.)
pgLazyQuery :: PGConnection -> PGPreparedQuery a -> Word32 -- ^ Chunk size (1 is common, 0 is all-or-nothing)
  -> IO [a]
pgLazyQuery c (QueryParser q p) count =
  fmap (p e) <$> pgPreparedLazyQuery c sql types bind bc count where
  e = pgTypeEnv c
  PreparedQuery sql types bind bc = q e

-- |Given a SQL statement with placeholders of the form @${expr}@, return a (hopefully) valid SQL statement with @$N@ placeholders and the list of expressions.
-- This does not understand strings or other SQL syntax, so any literal occurrence of the string @${@ must be escaped as @$${@.
-- Embedded expressions may not contain @{@ or @}@.
sqlPlaceholders :: String -> (String, [String])
sqlPlaceholders = sph (1 :: Int) where
  sph n ('$':'$':'{':s) = first (('$':) . ('{':)) $ sph n s
  sph n ('$':'{':s)
    | (e, '}':r) <- break (\c -> c == '{' || c == '}') s =
      (('$':show n) ++) *** (e :) $ sph (succ n) r
    | otherwise = error $ "Error parsing SQL statement: could not find end of expression: ${" ++ s
  sph n (c:s) = first (c:) $ sph n s
  sph _ "" = ("", [])

-- |Given a SQL statement with placeholders of the form @$N@ and a list of TH 'String' expressions, return a new 'String' expression that substitutes the expressions for the placeholders.
-- This does not understand strings or other SQL syntax, so any literal occurrence of a string like @$N@ must be escaped as @$$N@.
sqlSubstitute :: String -> [TH.Exp] -> TH.Exp
sqlSubstitute sql exprl = ss sql where
  bnds = (1, length exprl)
  exprs = listArray bnds exprl
  expr n
    | inRange bnds n = exprs ! n
    | otherwise = error $ "SQL placeholder '$" ++ show n ++ "' out of range (not recognized by PostgreSQL); literal occurrences may need to be escaped with '$$'"
  ss ('$':'$':d:r) | isDigit d = ['$',d] ++$ ss r
  ss ('$':s@(d:_)) | isDigit d, [(n, r)] <- readDec s = expr n $++$ ss r
  ss (c:r) = [c] ++$ ss r
  ss "" = stringE ""

splitCommas :: String -> [String]
splitCommas = spl where
  spl [] = []
  spl [c] = [[c]]
  spl (',':s) = "":spl s
  spl (c:s) = (c:h):t where h:t = spl s

trim :: String -> String
trim = dropWhileEnd isSpace . dropWhile isSpace

-- |Flags affecting how and what type of query to build with 'makePGQuery'.
data QueryFlags = QueryFlags
  { flagQuery :: Bool -- ^ Create a query -- otherwise just call 'pgSubstituteLiterals' to create a string (SQL fragment).
  , flagNullable :: Maybe Bool -- ^ Disable nullability inference, treating all values as nullable (if 'True') or not (if 'False').
  , flagPrepare :: Maybe [String] -- ^ Prepare and re-use query, binding parameters of the given types (inferring the rest, like PREPARE).
  }

-- |'QueryFlags' for a default (simple) query.
simpleQueryFlags :: QueryFlags
simpleQueryFlags = QueryFlags True Nothing Nothing

-- |Construct a 'PGQuery' from a SQL string.
makePGQuery :: QueryFlags -> String -> TH.ExpQ
makePGQuery QueryFlags{ flagQuery = False } sqle = pgSubstituteLiterals sqle
makePGQuery QueryFlags{ flagNullable = nulls, flagPrepare = prep } sqle = do
  (pt, rt) <- TH.runIO $ tpgDescribe sqlp (fromMaybe [] prep) (isNothing nulls)
  when (length pt < length exprs) $ fail "Not all expression placeholders were recognized by PostgreSQL; literal occurrences of '${' may need to be escaped with '$${'"

  e <- TH.newName "_tenv"
  (vars, vals) <- mapAndUnzipM (\t -> do
    v <- TH.newName $ 'p':tpgValueName t
    return 
      ( TH.VarP v
      , tpgTypeEncoder (isNothing prep) t e `TH.AppE` TH.VarE v
      )) pt
  (pats, conv, bins) <- unzip3 <$> mapM (\t -> do
    v <- TH.newName $ 'c':tpgValueName t
    return 
      ( TH.VarP v
      , tpgTypeDecoder (Fold.and nulls) t e `TH.AppE` TH.VarE v
      , tpgTypeBinary t e
      )) rt
  foldl TH.AppE (TH.LamE vars $ TH.ConE 'QueryParser
    `TH.AppE` TH.LamE [TH.VarP e] (if isNothing prep
      then TH.ConE 'SimpleQuery
        `TH.AppE` sqlSubstitute sqlp vals
      else TH.ConE 'PreparedQuery
        `TH.AppE` stringE sqlp 
        `TH.AppE` TH.ListE (map (TH.LitE . TH.IntegerL . toInteger . tpgValueTypeOID) pt) 
        `TH.AppE` TH.ListE vals 
        `TH.AppE` TH.ListE 
#ifdef USE_BINARY
          bins
#else
          []
#endif
      )
    `TH.AppE` TH.LamE [TH.VarP e, TH.ListP pats] (TH.TupE conv))
    <$> mapM parse exprs
  where
  (sqlp, exprs) = sqlPlaceholders sqle
  parse e = either (fail . (++) ("Failed to parse expression {" ++ e ++ "}: ")) return $ parseExp e

-- |Parse flags off the beginning of a query string, returning the flags and the remaining string.
parseQueryFlags :: String -> (QueryFlags, String)
parseQueryFlags = pqf simpleQueryFlags where
  pqf f@QueryFlags{ flagQuery = True, flagPrepare = Nothing } ('#':q) = pqf f{ flagQuery = False } q
  pqf f@QueryFlags{ flagQuery = True, flagNullable = Nothing } ('?':q) = pqf f{ flagNullable = Just True } q
  pqf f@QueryFlags{ flagQuery = True, flagNullable = Nothing } ('!':q) = pqf f{ flagNullable = Just False } q
  pqf f@QueryFlags{ flagQuery = True, flagPrepare = Nothing } ('$':q) = pqf f{ flagPrepare = Just [] } q
  pqf f@QueryFlags{ flagQuery = True, flagPrepare = Just [] } ('(':s) = pqf f{ flagPrepare = Just args } (sql r) where
    args = map trim $ splitCommas arg
    (arg, r) = break (')' ==) s
    sql (')':q) = q
    sql _ = error "pgSQL: unterminated argument list" 
  pqf f q = (f, q)

qqQuery :: String -> TH.ExpQ
qqQuery = uncurry makePGQuery . parseQueryFlags

qqTop :: Bool -> String -> TH.DecsQ
qqTop True ('!':sql) = qqTop False sql
qqTop err sql = do
  r <- TH.runIO $ try $ withTPGConnection $ \c ->
    pgSimpleQuery c sql
  either ((if err then TH.reportError else TH.reportWarning) . (show :: PGError -> String)) (const $ return ()) r
  return []

-- |A quasi-quoter for PGSQL queries.
--
-- Used in expression context, it may contain any SQL statement @[pgSQL|SELECT ...|]@.
-- The statement may contain PostgreSQL-style placeholders (@$1@, @$2@, ...) or in-line placeholders (@${1+1}@) containing any valid Haskell expression (except @{}@).
-- It will be replaced by a 'PGQuery' object that can be used to perform the SQL statement.
-- If there are more @$N@ placeholders than expressions, it will instead be a function accepting the additional parameters and returning a 'PGQuery'.
-- 
-- Note that all occurrences of @$N@ or @${@ will be treated as placeholders, regardless of their context in the SQL (e.g., even within SQL strings or other places placeholders are disallowed by PostgreSQL), which may cause invalid SQL or other errors.
-- If you need to pass a literal @$@ through in these contexts, you may double it to escape it as @$$N@ or @$${@.
--
-- The statement may start with one of more special flags affecting the interpretation:
--
-- [@?@] To disable nullability inference, treating all result values as nullable, thus returning 'Maybe' values regardless of inferred nullability. This makes unexpected NULL errors impossible.
-- [@!@] To disable nullability inference, treating all result values as /not/ nullable, thus only returning 'Maybe' where requested. This is makes unexpected NULL errors more likely.
-- [@$@] To create a 'PGPreparedQuery' rather than a 'PGSimpleQuery', by default inferring parameter types.
-- [@$(type,...)@] To specify specific types to a prepared query (see <http://www.postgresql.org/docs/current/static/sql-prepare.html> for details).
-- [@#@] Only do literal @${}@ substitution using 'pgSubstituteLiterals' and return a string, not a query.
-- 
-- 'pgSQL' can also be used at the top-level to execute SQL statements at compile-time (without any parameters and ignoring results).
-- Here the query can only be prefixed with @!@ to make errors non-fatal.
pgSQL :: QuasiQuoter
pgSQL = QuasiQuoter
  { quoteExp = qqQuery
  , quoteType = const $ fail "pgSQL not supported in types"
  , quotePat = const $ fail "pgSQL not supported in patterns"
  , quoteDec = qqTop True
  }
