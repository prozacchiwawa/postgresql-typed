{-# LANGUAGE PatternGuards, MultiParamTypeClasses, FunctionalDependencies, TypeSynonymInstances, FlexibleInstances, FlexibleContexts #-}
module Database.TemplatePG.Query
  ( PGQuery(..)
  , PGSimpleQuery
  , PGPreparedQuery
  , rawPGSimpleQuery
  , rawPGPreparedQuery
  , QueryFlags(..)
  , simpleFlags
  , makePGQuery
  , pgSQL
  , pgExecute
  , pgQuery
  , pgLazyQuery
  ) where

import Control.Applicative ((<$>))
import Control.Arrow ((***), first, second)
import Control.Monad (when, mapAndUnzipM)
import Data.Array (listArray, (!), inRange)
import Data.Char (isDigit)
import Data.Maybe (fromMaybe)
import Data.Word (Word32)
import Language.Haskell.Meta.Parse (parseExp)
import qualified Language.Haskell.TH as TH
import Language.Haskell.TH.Quote (QuasiQuoter(..))
import Numeric (readDec)

import Database.TemplatePG.Types
import Database.TemplatePG.Protocol
import Database.TemplatePG.Connection

class PGQuery q a | q -> a where
  -- |Execute a query and return the number of rows affected (or -1 if not known) and a list of results.
  pgRunQuery :: PGConnection -> q -> IO (Int, [a])
class PGQuery q PGData => PGRawQuery q

-- |Execute a query that does not return result.
-- Return the number of rows affected (or -1 if not known).
pgExecute :: PGQuery q () => PGConnection -> q -> IO Int
pgExecute c q = fst <$> pgRunQuery c q

-- |Run a query and return a list of row results.
pgQuery :: PGQuery q a => PGConnection -> q -> IO [a]
pgQuery c q = snd <$> pgRunQuery c q


data SimpleQuery = SimpleQuery String
instance PGQuery SimpleQuery PGData where
  pgRunQuery c (SimpleQuery sql) = pgSimpleQuery c sql
instance PGRawQuery SimpleQuery where


data PreparedQuery = PreparedQuery String PGData
instance PGQuery PreparedQuery PGData where
  pgRunQuery c (PreparedQuery sql bind) = pgPreparedQuery c sql bind
instance PGRawQuery PreparedQuery where


data QueryParser q a = QueryParser q (PGData -> a)
instance PGRawQuery q => PGQuery (QueryParser q a) a where
  pgRunQuery c (QueryParser q p) = second (map p) <$> pgRunQuery c q

instance Functor (QueryParser q) where
  fmap f (QueryParser q p) = QueryParser q (f . p)

rawParser :: q -> QueryParser q PGData
rawParser q = QueryParser q id

-- |A simple one-shot query that simply substitutes literal representations of parameters for placeholders.
type PGSimpleQuery = QueryParser SimpleQuery
-- |A prepared query that automatically is prepared in the database the first time it is run and bound with new parameters each subsequent time.
type PGPreparedQuery = QueryParser PreparedQuery

-- |Make a simple query directly from a query string, with no type inference
rawPGSimpleQuery :: String -> PGSimpleQuery PGData
rawPGSimpleQuery = rawParser . SimpleQuery

-- |Make a prepared query directly from a query string and bind parameters, with no type inference
rawPGPreparedQuery :: String -> PGData -> PGPreparedQuery PGData
rawPGPreparedQuery sql = rawParser . PreparedQuery sql

-- |Run a prepared query in lazy mode, where only chunk size rows are requested at a time.
-- If you eventually retrieve all the rows this way, it will be far less efficient than using @pgQuery@, since every chunk requires an additional round-trip.
-- Although you may safely stop consuming rows early, currently you may not interleave any other database operation while reading rows.  (This limitation could theoretically be lifted if required.)
pgLazyQuery :: PGConnection -> PGPreparedQuery a -> Word32 -- ^ Chunk size (1 is common, 0 is all-or-nothing)
  -> IO [a]
pgLazyQuery c (QueryParser (PreparedQuery sql bind) p) count =
  fmap p <$> pgPreparedLazyQuery c sql bind count

-- |Given a result description, create a function to convert a result to a
-- tuple.
convertRow :: [(String, PGTypeHandler, Bool)] -- ^ result description
           -> TH.Q TH.Exp -- ^ A function for converting a row of the given result description
convertRow types = do
  (pats, conv) <- mapAndUnzipM (\t@(n, _, _) -> do
    v <- TH.newName n
    return (TH.varP v, convertColumn (TH.varE v) t)) types
  TH.lamE [TH.listP pats] $ TH.tupE conv

-- |Given a raw PostgreSQL result and a result field type, convert the
-- field to a Haskell value.
-- If the boolean
-- argument is 'False', that means that we know that the value is not nullable
-- and we can use 'fromJust' to keep the code simple. If it's 'True', then we
-- don't know if the value is nullable and must return a 'Maybe' value in case
-- it is.
convertColumn :: TH.ExpQ -- ^ the name of the variable containing the column value (of 'Maybe' 'ByteString')
              -> (String, PGTypeHandler, Bool) -- ^ the result field type
              -> TH.ExpQ
convertColumn v (n, t, False) = [| $(pgTypeDecoder t) (fromMaybe (error $(TH.litE $ TH.stringL $ "Unexpected NULL value in " ++ n)) $(v)) |]
convertColumn v (_, t, True) = [| fmap $(pgTypeDecoder t) $(v) |]

-- |Given a SQL statement with placeholders of the form @${expr}@, return a (hopefully) valid SQL statement with @$N@ placeholders and the list of expressions.
-- This does not understand strings or other SQL syntax, so any literal occurrence of the string @${@ must be escaped as @$${@.
-- Embedded expressions may not contain @{@ or @}@.
sqlPlaceholders :: String -> (String, [String])
sqlPlaceholders = sph 1 where
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
sqlSubstitute sql exprl = se sql where
  bnds = (1, length exprl)
  exprs = listArray bnds exprl
  expr n
    | inRange bnds n = exprs ! n
    | otherwise = error $ "SQL placeholder '$" ++ show n ++ "' out of range (not recognized by PostgreSQL); literal occurrences may need to be escaped with '$$'"

  se = uncurry ((+$+) . stringL) . ss
  ss ('$':'$':d:r) | isDigit d = first (('$':) . (d:)) $ ss r
  ss ('$':s@(d:_)) | isDigit d, [(n, r)] <- readDec s = ("", expr n +$+ se r)
  ss (c:r) = first (c:) $ ss r
  ss "" = ("", stringL "")

stringL :: String -> TH.Exp
stringL = TH.LitE . TH.StringL

(+$+) :: TH.Exp -> TH.Exp -> TH.Exp
infixr 5 +$+
TH.LitE (TH.StringL "") +$+ e = e
e +$+ TH.LitE (TH.StringL "") = e
TH.LitE (TH.StringL l) +$+ TH.LitE (TH.StringL r) = stringL (l ++ r)
l +$+ r = TH.InfixE (Just l) (TH.VarE '(++)) (Just r)

data QueryFlags = QueryFlags
  { flagNullable :: Bool
  , flagPrepared :: Bool
  }

simpleFlags :: QueryFlags
simpleFlags = QueryFlags False False

-- |Construct a 'PGQuery' from a SQL string.
makePGQuery :: QueryFlags -> String -> TH.ExpQ
makePGQuery QueryFlags{ flagNullable = nulls, flagPrepared = prep } sqle = do
  (pt, rt) <- TH.runIO $ withTHConnection $ \c -> pgDescribe c sqlp (not nulls)
  when (length pt < length exprs) $ fail "Not all expression placeholders were recognized by PostgreSQL; literal occurrences of '${' may need to be escaped with '$${'"

  (vars, vals) <- mapAndUnzipM (\t -> do
    v <- TH.newName "p"
    (,) (TH.VarP v) . (`TH.AppE` TH.VarE v) <$> encf t) pt
  conv <- convertRow rt
  let pgq
        | prep = TH.ConE 'PreparedQuery `TH.AppE` stringL sqlp `TH.AppE` TH.ListE vals
        | otherwise = TH.ConE 'SimpleQuery `TH.AppE` sqlSubstitute sqlp vals
  foldl TH.AppE (TH.LamE vars $ TH.ConE 'QueryParser `TH.AppE` pgq `TH.AppE` conv) <$> mapM parse exprs
  where
  (sqlp, exprs) = sqlPlaceholders sqle
  parse e = either (fail . (++) ("Failed to parse expression {" ++ e ++ "}: ")) return $ parseExp e
  encf
    | prep = pgTypeEncoder
    | otherwise = pgTypeEscaper

qqQuery :: QueryFlags -> String -> TH.ExpQ
qqQuery f@QueryFlags{ flagNullable = False } ('?':q) = qqQuery f{ flagNullable = True } q
qqQuery f@QueryFlags{ flagPrepared = False } ('$':q) = qqQuery f{ flagPrepared = True } q
qqQuery f q = makePGQuery f q

qqType :: String -> TH.TypeQ
qqType t = fmap pgTypeType $ TH.runIO $ withTHConnection $ \c ->
  maybe (fail $ "Unknown PostgreSQL type: " ++ t) (getPGType c . fst) =<< getTypeOID c t

-- |A quasi-quoter for PGSQL queries.
--
-- Used in expression context, it may contain any SQL statement @[pgSQL|SELECT ...|]@.
-- The statement may contain PostgreSQL-style placeholders (@$1@, @$2@, ...) or in-line placeholders (@${1+1}@) containing any valid Haskell expression (except @{}@).
-- It will be replaced by a 'PGQuery' object that can be used to perform the SQL statement.
-- If there are more @$N@ placeholders than expressions, it will instead be a function accepting the additional parameters and returning a 'PGQuery'.
-- Note that all occurrences of @$N@ or @${@ will be treated as placeholders, regardless of their context in the SQL (e.g., even within SQL strings or other places placeholders are disallowed by PostgreSQL), which may cause invalid SQL or other errors.
-- If you need to pass a literal @$@ through in these contexts, you may double it to escape it as @$$N@ or @$${@.
--
-- The statement may start with one of more special flags affecting the interpretation:
--
-- [@$@] To create a 'PGPreparedQuery' rather than a 'PGSimpleQuery'
-- [@?@] To treat all result values as nullable, thus returning 'Maybe' values regardless of inferred nullability.
--
-- In type context, [pgSQL|typname|] will be replaced with the Haskell type that corresponds to PostgreSQL type @typname@.
pgSQL :: QuasiQuoter
pgSQL = QuasiQuoter
  { quoteExp = qqQuery simpleFlags
  , quoteType = qqType
  , quotePat = const $ fail "pgSQL not supported in patterns"
  , quoteDec = const $ fail "pgSQL not supported at top level"
  }
