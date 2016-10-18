{-# LANGUAGE CPP, TemplateHaskell, FlexibleInstances, MultiParamTypeClasses, DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
-- |
-- Module: Database.PostgreSQL.Typed.Enum
-- Copyright: 2015 Dylan Simon
-- 
-- Support for PostgreSQL enums.

module Database.PostgreSQL.Typed.Enum 
  ( PGEnum
  , pgEnumValues
  , dataPGEnum
  , makePGEnum
  ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as BSL
import Data.Ix (Ix)
import Data.Typeable (Typeable)
import qualified Language.Haskell.TH as TH

import Database.PostgreSQL.Typed.Types
import Database.PostgreSQL.Typed.Dynamic
import Database.PostgreSQL.Typed.Protocol
import Database.PostgreSQL.Typed.TypeCache
import Database.PostgreSQL.Typed.TH

-- |A type based on a PostgreSQL enum. Automatically instantiated by 'dataPGEnum'.
class (Eq a, Ord a, Enum a, Bounded a, Show a) => PGEnum a

-- |List of all the values in the enum along with their database names.
pgEnumValues :: PGEnum a => [(a, String)]
pgEnumValues = map (\e -> (e, show e)) $ enumFromTo minBound maxBound

-- |Create a new enum type corresponding to the given PostgreSQL enum type.
-- For example, if you have @CREATE TYPE foo AS ENUM (\'abc\', \'DEF\');@, then
-- @dataPGEnum \"Foo\" \"foo\" (\"Foo_\"++)@ will be equivalent to:
-- 
-- > data Foo = Foo_abc | Foo_DEF deriving (Eq, Ord, Enum, Bounded, Typeable)
-- > instance Show Foo where show Foo_abc = "abc" ...
-- > instance PGType "foo" where PGVal "foo" = Foo
-- > instance PGParameter "foo" Foo where ...
-- > instance PGColumn "foo" Foo where ...
-- > instance PGRep Foo where PGRepType = "foo"
-- > instance PGEnum Foo where pgEnumValues = [(Foo_abc, "abc"), (Foo_DEF, "DEF")]
--
-- Requires language extensions: TemplateHaskell, FlexibleInstances, MultiParamTypeClasses, DeriveDataTypeable, DataKinds, TypeFamilies
dataPGEnum :: String -- ^ Haskell type to create 
  -> String -- ^ PostgreSQL enum type name
  -> (String -> String) -- ^ How to generate constructor names from enum values, e.g. @(\"Type_\"++)@
  -> TH.DecsQ
dataPGEnum typs pgenum valnf = do
  (pgid, vals) <- TH.runIO $ withTPGTypeConnection $ \tpg -> do
    vals <- map (\([eo, PGTextValue v]) -> (pgDecodeRep eo, v)) . snd
      <$> pgSimpleQuery (pgConnection tpg) (BSL.fromChunks 
        [ "SELECT enumtypid, enumlabel"
        ,  " FROM pg_catalog.pg_enum"
        , " WHERE enumtypid = ", pgLiteralRep pgenum, "::regtype"
        , " ORDER BY enumsortorder"
        ])
    case vals of
      [] -> fail $ "dataPGEnum " ++ typs ++ " = " ++ pgenum ++ ": no values found"
      (eo, _):_ -> do
        et <- maybe (fail $ "dataPGEnum " ++ typs ++ " = " ++ pgenum ++ ": enum type not found (you may need to use reloadTPGTypes or adjust search_path)") return
          =<< lookupPGType tpg eo
        return (et, map snd vals)
  let valn = map (\v -> let u = BSC.unpack v in (TH.mkName $ valnf u, map (TH.IntegerL . fromIntegral) $ BS.unpack v, TH.StringL u)) vals
      typl = TH.LitT (TH.StrTyLit pgid)
  dv <- TH.newName "x"
  return
    [ TH.DataD [] typn []
#if MIN_VERSION_template_haskell(2,11,0)
      Nothing
#endif
      (map (\(n, _, _) -> TH.NormalC n []) valn) $
#if MIN_VERSION_template_haskell(2,11,0)
      map TH.ConT
#endif
      [''Eq, ''Ord, ''Enum, ''Ix, ''Bounded, ''Typeable]
    , instanceD [] (TH.ConT ''Show `TH.AppT` typt)
      [ TH.FunD 'show $ map (\(n, _, v) -> TH.Clause [TH.ConP n []]
        (TH.NormalB $ TH.LitE v) []) valn
      ]
    , instanceD [] (TH.ConT ''PGType `TH.AppT` typl)
      [ TH.TySynInstD ''PGVal $ TH.TySynEqn [typl] typt
      ]
    , instanceD [] (TH.ConT ''PGParameter `TH.AppT` typl `TH.AppT` typt)
      [ TH.FunD 'pgEncode $ map (\(n, l, _) -> TH.Clause [TH.WildP, TH.ConP n []]
        (TH.NormalB $ TH.VarE 'BS.pack `TH.AppE` TH.ListE (map TH.LitE l)) []) valn
      ]
    , instanceD [] (TH.ConT ''PGColumn `TH.AppT` typl `TH.AppT` typt)
      [ TH.FunD 'pgDecode [TH.Clause [TH.WildP, TH.VarP dv]
        (TH.NormalB $ TH.CaseE (TH.VarE 'BS.unpack `TH.AppE` TH.VarE dv) $ map (\(n, l, _) ->
          TH.Match (TH.ListP (map TH.LitP l)) (TH.NormalB $ TH.ConE n) []) valn ++
          [TH.Match TH.WildP (TH.NormalB $ TH.AppE (TH.VarE 'error) $
            TH.InfixE (Just $ TH.LitE (TH.StringL ("pgDecode " ++ pgid ++ ": "))) (TH.VarE '(++)) (Just $ TH.VarE 'BSC.unpack `TH.AppE` TH.VarE dv))
            []])
        []] 
      ]
    , instanceD [] (TH.ConT ''PGRep `TH.AppT` typt)
      [ TH.TySynInstD ''PGRepType $ TH.TySynEqn [typt] typl
      ]
    , instanceD [] (TH.ConT ''PGEnum `TH.AppT` typt) []
    ]
  where
  typn = TH.mkName typs
  typt = TH.ConT typn
  instanceD = TH.InstanceD
#if MIN_VERSION_template_haskell(2,11,0)
      Nothing
#endif

-- |A deprecated alias for 'dataPGEnum' with its arguments flipped.
makePGEnum :: String -> String -> (String -> String) -> TH.DecsQ
makePGEnum = flip dataPGEnum
