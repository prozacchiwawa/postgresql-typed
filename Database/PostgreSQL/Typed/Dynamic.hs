{-# LANGUAGE CPP, FlexibleContexts, FlexibleInstances, MultiParamTypeClasses, ScopedTypeVariables, DataKinds, DefaultSignatures, TemplateHaskell, TypeFamilies #-}
-- |
-- Module: Database.PostgreSQL.Typed.Dynamic
-- Copyright: 2015 Dylan Simon
-- 
-- Automatic (dynamic) marshalling of PostgreSQL values based on Haskell types (not SQL statements).
-- This is intended for direct construction of queries and query data, bypassing the normal SQL type inference.

module Database.PostgreSQL.Typed.Dynamic 
  ( PGRep(..)
  , pgLiteralString
  , pgSafeLiteral
  , pgSafeLiteralString
  , pgSubstituteLiterals
  ) where

#if !MIN_VERSION_base(4,8,0)
import Control.Applicative ((<$>))
#endif
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as BSL
import Data.Monoid ((<>))
import Data.Int
#ifdef USE_SCIENTIFIC
import Data.Scientific (Scientific)
#endif
import Data.String (fromString)
#ifdef USE_TEXT
import qualified Data.Text as T
#endif
import qualified Data.Time as Time
#ifdef USE_UUID
import qualified Data.UUID as UUID
#endif
import GHC.TypeLits (Symbol)
import Language.Haskell.Meta.Parse (parseExp)
import qualified Language.Haskell.TH as TH

import Database.PostgreSQL.Typed.Types
import Database.PostgreSQL.Typed.SQLToken

-- |Represents canonical/default PostgreSQL representation for various Haskell types, allowing convenient type-driven marshalling.
class (PGParameter (PGRepType a) a, PGColumn (PGRepType a) a) => PGRep a where
  -- |The PostgreSOL type that this type should be converted to.
  type PGRepType a :: Symbol
  pgTypeOf :: a -> PGTypeName (PGRepType a)
  pgTypeOf _ = PGTypeProxy
  -- |Encode a value, using 'pgEncodeValue' by default.
  pgEncodeRep :: a -> PGValue
  pgEncodeRep x = pgEncodeValue unknownPGTypeEnv (pgTypeOf x) x
  -- |Produce a literal value for interpolation in a SQL statement, using 'pgLiteral' by default.  Using 'pgSafeLiteral' is usually safer as it includes type cast.
  pgLiteralRep :: a -> BS.ByteString
  pgLiteralRep x = pgLiteral (pgTypeOf x) x
  -- |Decode a value, using 'pgDecode' for text values or producing an error for binary or null values by default.
  pgDecodeRep :: PGValue -> a
  pgDecodeRep (PGTextValue v) = pgDecode (PGTypeProxy :: PGTypeName (PGRepType a)) v
  pgDecodeRep (PGBinaryValue v) = pgDecodeBinary unknownPGTypeEnv (PGTypeProxy :: PGTypeName (PGRepType a)) v
  pgDecodeRep _ = error $ "pgDecodeRep " ++ pgTypeName (PGTypeProxy :: PGTypeName (PGRepType a)) ++ ": unsupported PGValue"

-- |Produce a raw SQL literal from a value. Using 'pgSafeLiteral' is usually safer when interpolating in a SQL statement.
pgLiteralString :: PGRep a => a -> String
pgLiteralString = BSC.unpack . pgLiteralRep

-- |Produce a safely type-cast literal value for interpolation in a SQL statement, e.g., "'123'::integer".
pgSafeLiteral :: PGRep a => a -> BS.ByteString
pgSafeLiteral x = pgLiteralRep x <> BSC.pack "::" <> fromString (pgTypeName (pgTypeOf x))

-- |Identical to @'BSC.unpack' . 'pgSafeLiteral'@ but more efficient.
pgSafeLiteralString :: PGRep a => a -> String
pgSafeLiteralString x = pgLiteralString x ++ "::" ++ pgTypeName (pgTypeOf x)

instance PGRep a => PGRep (Maybe a) where
  type PGRepType (Maybe a) = PGRepType a
  pgEncodeRep Nothing = PGNullValue
  pgEncodeRep (Just x) = pgEncodeRep x
  pgLiteralRep Nothing = BSC.pack "NULL"
  pgLiteralRep (Just x) = pgLiteralRep x
  pgDecodeRep PGNullValue = Nothing
  pgDecodeRep v = Just (pgDecodeRep v)

instance PGRep Bool where
  type PGRepType Bool = "boolean"
instance PGRep OID where
  type PGRepType OID = "oid"
instance PGRep Int16 where
  type PGRepType Int16 = "smallint"
instance PGRep Int32 where
  type PGRepType Int32 = "integer"
instance PGRep Int64 where
  type PGRepType Int64 = "bigint"
instance PGRep Float where
  type PGRepType Float = "real"
instance PGRep Double where
  type PGRepType Double = "double precision"
instance PGRep Char where
  type PGRepType Char = "\"char\""
instance PGRep String where
  type PGRepType String = "text"
instance PGRep BS.ByteString where
  type PGRepType BS.ByteString = "text"
#ifdef USE_TEXT
instance PGRep T.Text where
  type PGRepType T.Text = "text"
#endif
instance PGRep Time.Day where
  type PGRepType Time.Day = "date"
instance PGRep Time.TimeOfDay where
  type PGRepType Time.TimeOfDay = "time without time zone"
instance PGRep (Time.TimeOfDay, Time.TimeZone) where
  type PGRepType (Time.TimeOfDay, Time.TimeZone) = "time with time zone"
instance PGRep Time.LocalTime where
  type PGRepType Time.LocalTime = "timestamp without time zone"
instance PGRep Time.UTCTime where
  type PGRepType Time.UTCTime = "timestamp with time zone"
instance PGRep Time.DiffTime where
  type PGRepType Time.DiffTime = "interval"
instance PGRep Rational where
  type PGRepType Rational = "numeric"
#ifdef USE_SCIENTIFIC
instance PGRep Scientific where
  type PGRepType Scientific = "numeric"
#endif
#ifdef USE_UUID
instance PGRep UUID.UUID where
  type PGRepType UUID.UUID = "uuid"
#endif

-- |Create an expression that literally substitutes each instance of @${expr}@ for the result of @pgSafeLiteral expr@, producing a lazy 'BSL.ByteString'.
-- This lets you do safe, type-driven literal substitution into SQL fragments without needing a full query, bypassing placeholder inference and any prepared queries, for example when using 'Database.PostgreSQL.Typed.Protocol.pgSimpleQuery' or 'Database.PostgreSQL.Typed.Protocol.pgSimpleQueries_'.
-- Unlike most other TH functions, this does not require any database connection.
pgSubstituteLiterals :: String -> TH.ExpQ
pgSubstituteLiterals sql = TH.AppE (TH.VarE 'BSL.fromChunks) . TH.ListE <$> mapM sst (sqlTokens sql) where
  sst (SQLExpr e) = do
    v <- either (fail . (++) ("Failed to parse expression {" ++ e ++ "}: ")) return $ parseExp e
    return $ TH.VarE 'pgSafeLiteral `TH.AppE` v
  sst t = return $ TH.VarE 'fromString `TH.AppE` TH.LitE (TH.StringL $ show t)
