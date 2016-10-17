{-# LANGUAGE CPP #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
-- |
-- Module: Database.PostgreSQL.Typed.Models
-- Copyright: 2016 Dylan Simon
-- 
-- Automatically create data models based on tables.

module Database.PostgreSQL.Typed.Models
  ( dataPGTable
  ) where

import qualified Data.ByteString.Lazy as BSL
import           Data.Maybe (fromMaybe)
import qualified Language.Haskell.TH as TH

import           Database.PostgreSQL.Typed.Types
import           Database.PostgreSQL.Typed.Dynamic
import           Database.PostgreSQL.Typed.Protocol
import           Database.PostgreSQL.Typed.TypeCache
import           Database.PostgreSQL.Typed.TH

-- |Create a new data type corresponding to the given PostgreSQL table.
-- For example, if you have @CREATE TABLE foo (abc integer NOT NULL, def text);@, then
-- @dataPGTable \"Foo\" \"foo\" (\"foo_\"++)@ will be equivalent to:
-- 
-- > data Foo = Foo{ foo_abc :: PGVal "integer", foo_def :: Maybe (PGVal "text") }
--
-- (Note that @type PGVal "integer" = Int32@ and @type PGVal "text" = Text@ by default.)
-- If you want any derived instances, you'll need to create them yourself using StandaloneDeriving.
--
-- Requires language extensions: TemplateHaskell, FlexibleInstances, MultiParamTypeClasses, DeriveDataTypeable, DataKinds, TypeFamilies
dataPGTable :: String -- ^ Haskell type and constructor to create 
  -> String -- ^ PostgreSQL table/relation name
  -> (String -> String) -- ^ How to generate field names from column names, e.g. @("table_" ++)@
  -> TH.DecsQ
dataPGTable dats tabs colf = do
  cols <- TH.runIO $ withTPGTypeConnection $ \tpg ->
    mapM (\(~[cn, ct, cnn]) ->
      let n = pgDecodeRep cn
          o = pgDecodeRep ct in
      (n, , pgDecodeRep cnn)
        . fromMaybe (error $ "dataPGTable " ++ dats ++ " = " ++ tabs ++ ": column '" ++ n ++ "' has unknown type " ++ show o)
        <$> lookupPGType tpg o)
      . snd
    =<< pgSimpleQuery (pgConnection tpg) (BSL.fromChunks
      [ "SELECT attname, atttypid, attnotnull"
      ,  " FROM pg_attribute"
      , " WHERE attrelid = ", pgLiteralRep tabs, "::regclass::oid"
      ,   " AND attnum > 0 AND NOT attisdropped"
      ,   " ORDER BY attrelid, attnum"
      ])
  return [TH.DataD
    []
    datn
    []
#if MIN_VERSION_template_haskell(2,11,0)
    Nothing
#endif
    [ TH.RecC datn $ map (\(cn, ct, cnn) ->
      ( TH.mkName (colf cn)
      , TH.Bang TH.NoSourceUnpackedness TH.NoSourceStrictness
      , (if cnn then id else TH.AppT (TH.ConT ''Maybe))
        (TH.ConT ''PGVal `TH.AppT` TH.LitT (TH.StrTyLit ct))))
      cols
    ]
    []]
  where
  datn = TH.mkName dats
