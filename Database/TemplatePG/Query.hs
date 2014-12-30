{-# LANGUAGE PatternGuards #-}
module Database.TemplatePG.Query
  ( PGStatement
  , PGSimpleQuery
  , PGPreparedStatement
  , PGQueryParser
  , pgRawParser
  , pgExecute
  , pgQuery
  , makePGSimpleQuery
  , makePGPreparedQuery
  ) where

import Control.Applicative ((<$>))
import Control.Arrow ((***), first)
import Control.Monad (when, mapAndUnzipM)
import Data.Array (listArray, (!), inRange)
import Data.Char (isDigit)
import Data.Maybe (fromMaybe)
import Language.Haskell.Meta.Parse (parseExp)
import qualified Language.Haskell.TH as TH
import Numeric (readDec)

import Database.TemplatePG.Types
import Database.TemplatePG.Protocol
import Database.TemplatePG.Connection

class PGStatement q where
  pgRawQuery :: PGConnection -> q -> IO (Int, [PGData])

pgRawExecute :: PGStatement q => PGConnection -> q -> IO Int
pgRawExecute c q = fst <$> pgRawQuery c q

data PGSimpleQuery = PGSimpleQuery String
instance PGStatement PGSimpleQuery where
  pgRawQuery c (PGSimpleQuery sql) = pgSimpleQuery c sql

data PGPreparedStatement = PGPreparedStatement String [PGData]

data PGQueryParser q a = PGQueryParser
  { pgQueryStatement :: q
  , pgQueryParser :: PGData -> a
  }
instance PGStatement q => PGStatement (PGQueryParser q a) where
  pgRawQuery c (PGQueryParser q _) = pgRawQuery c q

instance Functor (PGQueryParser q) where
  fmap f q = q{ pgQueryParser = f . pgQueryParser q }

pgRawParser :: q -> PGQueryParser q PGData
pgRawParser q = PGQueryParser q id

-- |Run a query and return a list of row results.
pgQuery :: PGStatement q => PGConnection -> PGQueryParser q a -> IO [a]
pgQuery c PGQueryParser{ pgQueryStatement = s, pgQueryParser = p } =
  map p . snd <$> pgRawQuery c s

-- |Execute a query that does not return result.
-- Return the number of rows affected (or -1 if not known).
pgExecute :: PGStatement q => PGConnection -> PGQueryParser q () -> IO Int
pgExecute = pgRawExecute


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
  foldl TH.AppE (TH.LamE vars $ TH.ConE 'PGQueryParser `TH.AppE` pgf sqlp vals `TH.AppE` conv) <$> mapM parse exprs
  where
  (sqlp, exprs) = sqlPlaceholders sqle
  parse e = either (fail . (++) ("Failed to parse expression {" ++ e ++ "}: ")) return $ parseExp e

-- |Produce a new PGQuery from a SQL query string.
-- This should be used as @$(makePGQuery \"SELECT ...\")@
makePGSimpleQuery :: String -> TH.Q TH.Exp
makePGSimpleQuery = makePGQuery pgTypeEscaper $ \sql ps ->
  TH.ConE 'PGSimpleQuery `TH.AppE` sqlSubstitute sql ps

makePGPreparedQuery :: String -> TH.Q TH.Exp
makePGPreparedQuery = makePGQuery pgTypeEncoder $ \sql ps ->
  TH.ConE 'PGPreparedStatement `TH.AppE` TH.LitE (TH.StringL sql) `TH.AppE` TH.ListE ps
