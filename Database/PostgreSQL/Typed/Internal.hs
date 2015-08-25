{-# LANGUAGE PatternSynonyms, PatternGuards, TemplateHaskell, GADTs, KindSignatures, DataKinds #-}
module Database.PostgreSQL.Typed.Internal
  ( stringE
  , pattern StringE
  , SQLSplit(..)
  , sqlSplitExprs
  , sqlSplitParams
  ) where

import Data.Char (isDigit)
import Data.String (IsString(..))
import qualified Language.Haskell.TH as TH
import Numeric (readDec)

stringE :: String -> TH.Exp
stringE = TH.LitE . TH.StringL

pattern StringE s = TH.LitE (TH.StringL s)

instance IsString TH.Exp where
  fromString = stringE

data SQLSplit a (literal :: Bool) where
  SQLLiteral :: String -> SQLSplit a 'False -> SQLSplit a 'True
  SQLPlaceholder :: a -> SQLSplit a 'True -> SQLSplit a 'False
  SQLSplitEnd :: SQLSplit a any

sqlCons :: Char -> SQLSplit a 'True -> SQLSplit a 'True
sqlCons c (SQLLiteral s l) = SQLLiteral (c : s) l
sqlCons c SQLSplitEnd = SQLLiteral [c] SQLSplitEnd

sqlSplitExprs :: String -> SQLSplit String 'True
sqlSplitExprs ('$':'$':'{':s) = sqlCons '$' $ sqlCons '{' $ sqlSplitExprs s
sqlSplitExprs ('$':'{':s)
  | (e, '}':r) <- break (\c -> c == '{' || c == '}') s = SQLLiteral "" $ SQLPlaceholder e $ sqlSplitExprs r
  | otherwise = error $ "Error parsing SQL: could not find end of expression: ${" ++ s
sqlSplitExprs (c:s) = sqlCons c $ sqlSplitExprs s
sqlSplitExprs [] = SQLSplitEnd

sqlSplitParams :: String -> SQLSplit Int 'True
sqlSplitParams ('$':'$':d:s) | isDigit d = sqlCons '$' $ sqlCons d $ sqlSplitParams s
sqlSplitParams ('$':s@(d:_)) | isDigit d, [(n, r)] <- readDec s = SQLLiteral "" $ SQLPlaceholder n $ sqlSplitParams r
sqlSplitParams (c:s) = sqlCons c $ sqlSplitParams s
sqlSplitParams [] = SQLSplitEnd

