-- |
-- Module: Database.TemplatePG.Enum
-- Copyright: 2015 Dylan Simon
-- 
-- Support for PostgreSQL enums.

module Database.TemplatePG.Enum 
  ( makePGEnum
  ) where

import Control.Applicative ((<$>))
import Control.Monad (when)
import Data.Foldable (toList)
import qualified Data.Sequence as Seq
import qualified Language.Haskell.TH as TH

import Database.TemplatePG.Protocol
import Database.TemplatePG.TH
import Database.TemplatePG.Types

-- |Create a new enum type corresponding to the given PostgreSQL enum type.
-- For example, if you have @CREATE TYPE foo AS ENUM (\'abc\', \'DEF\');@, then
-- @makePGEnum \"foo\" \"Foo\" (\"Foo_\"++)@ will be equivalent to:
-- 
-- @
-- data Foo = Foo_abc | Foo_DEF deriving (Eq, Ord, Enum, Bounded)
-- instance PGType Foo where ...
-- registerPGType \"foo\" (ConT ''Foo)
-- @
makePGEnum :: String -- ^ PostgreSQL enum type name
  -> String -- ^ Haskell type to create
  -> (String -> String) -- ^ How to generate constructor names from enum values, e.g. @(\"Type_\"++)@
  -> TH.DecsQ
makePGEnum name typs valf = do
  (_, vals) <- TH.runIO $ withTPGConnection $ \c ->
    pgSimpleQuery c $ "SELECT enumlabel FROM pg_catalog.pg_enum JOIN pg_catalog.pg_type ON pg_enum.enumtypid = pg_type.oid WHERE typtype = 'e' AND typname = " ++ pgLiteral name ++ " ORDER BY enumsortorder"
  when (Seq.null vals) $ fail $ "makePGEnum: enum " ++ name ++ " not found"
  let 
    valn = map (\[Just v] -> let s = pgDecodeBS v in (TH.StringL s, TH.mkName $ valf s)) $ toList vals
  (++)
    [ TH.DataD [] typn [] (map (\(_, n) -> TH.NormalC n []) valn) [''Eq, ''Ord, ''Enum, ''Bounded]
    , TH.InstanceD [] (TH.AppT (TH.ConT ''PGType) typt)
      [ TH.FunD 'pgDecode $ map (\(l, n) -> TH.Clause [TH.LitP l] (TH.NormalB (TH.ConE n)) []) valn
      , TH.FunD 'pgEncode $ map (\(l, n) -> TH.Clause [TH.ConP n []] (TH.NormalB (TH.LitE l)) []) valn
      ]
    ] <$> registerTPGType name typt
  where
  typn = TH.mkName typs
  typt = TH.ConT typn
