{-# LANGUAGE PatternGuards, MultiParamTypeClasses, FunctionalDependencies, TypeSynonymInstances, FlexibleInstances, FlexibleContexts #-}
module Database.TemplatePG.Query
  ( PGQuery(..)
  , PGSimpleQuery
  , PGPreparedQuery
  , rawPGSimpleQuery
  , rawPGPreparedQuery
  , makePGSimpleQuery
  , makePGPreparedQuery
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
import Numeric (readDec)

import Database.TemplatePG.Types
import Database.TemplatePG.Protocol
import Database.TemplatePG.Connection

class PGQuery q a | q -> a where
  -- |Execute a query and return the number of rows affected (or -1 if not known) and a list of results.
  pgRunQuery :: PGConnection -> q -> IO (Int, [a])

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


data PreparedQuery = PreparedQuery String PGData
instance PGQuery PreparedQuery PGData where
  pgRunQuery c (PreparedQuery sql bind) = pgPreparedQuery c sql bind


data QueryParser q a b = QueryParser q (a -> b)
instance PGQuery q a => PGQuery (QueryParser q a b) b where
  pgRunQuery c (QueryParser q p) = second (map p) <$> pgRunQuery c q

instance Functor (QueryParser q a) where
  fmap f (QueryParser q p) = QueryParser q (f . p)

idParser :: q -> QueryParser q a a
idParser q = QueryParser q id

type PGSimpleQuery = QueryParser SimpleQuery PGData
type PGPreparedQuery = QueryParser PreparedQuery PGData

-- Make a simple query directly from a query string, with no type inference
rawPGSimpleQuery :: String -> PGSimpleQuery PGData
rawPGSimpleQuery = idParser . SimpleQuery

-- Make a prepared query directly from a query string and bind parameters, with no type inference
rawPGPreparedQuery :: String -> PGData -> PGPreparedQuery PGData
rawPGPreparedQuery sql = idParser . PreparedQuery sql

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
convertColumn v (n, t, False) = [| $(pgTypeDecoder t) (fromMaybe (error $ "Unexpected NULL value in " ++ n) $(v)) |]
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
    | otherwise = error $ "SQL placeholder '$" ++ show n ++ "' out of range (not recognized by PostgreSQL); literal occurances may need to be escaped with '$$'"

  se = uncurry ((+$+) . lit) . ss
  ss ('$':'$':d:r) | isDigit d = first (('$':) . (d:)) $ ss r
  ss ('$':s@(d:_)) | isDigit d, [(n, r)] <- readDec s = ("", expr n +$+ se r)
  ss (c:r) = first (c:) $ ss r
  ss "" = ("", lit "")

  lit = TH.LitE . TH.StringL
  TH.LitE (TH.StringL "") +$+ e = e
  e +$+ TH.LitE (TH.StringL "") = e
  TH.LitE (TH.StringL l) +$+ TH.LitE (TH.StringL r) = lit (l ++ r)
  l +$+ r = TH.InfixE (Just l) (TH.VarE '(++)) (Just r)

makePGQuery :: (PGTypeHandler -> TH.ExpQ) -> (String -> [TH.Exp] -> TH.Exp) -> String -> TH.ExpQ -- ^ a PGQuery
makePGQuery encf pgf sqle = do
  (pt, rt) <- TH.runIO $ withTHConnection $ \c -> pgDescribe c sqlp
  when (length pt < length exprs) $ fail "Not all expression placeholders were recognized by PostgreSQL; literal occurances of '${' may need to be escaped with '$${'"

  (vars, vals) <- mapAndUnzipM (\t -> do
    v <- TH.newName "p"
    (,) (TH.VarP v) . (`TH.AppE` TH.VarE v) <$> encf t) pt
  conv <- convertRow rt
  foldl TH.AppE (TH.LamE vars $ TH.ConE 'QueryParser `TH.AppE` pgf sqlp vals `TH.AppE` conv) <$> mapM parse exprs
  where
  (sqlp, exprs) = sqlPlaceholders sqle
  parse e = either (fail . (++) ("Failed to parse expression {" ++ e ++ "}: ")) return $ parseExp e

-- |Produce a new PGQuery from a SQL query string.
-- This should be used as @$(makePGQuery \"SELECT ...\")@
makePGSimpleQuery :: String -> TH.Q TH.Exp
makePGSimpleQuery = makePGQuery pgTypeEscaper $ \sql ps ->
  TH.ConE 'SimpleQuery `TH.AppE` sqlSubstitute sql ps

makePGPreparedQuery :: String -> TH.Q TH.Exp
makePGPreparedQuery = makePGQuery pgTypeEncoder $ \sql ps ->
  TH.ConE 'PreparedQuery `TH.AppE` TH.LitE (TH.StringL sql) `TH.AppE` TH.ListE ps
