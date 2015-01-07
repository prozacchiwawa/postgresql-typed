{-# LANGUAGE CPP, FlexibleInstances, MultiParamTypeClasses, ScopedTypeVariables, FunctionalDependencies, UndecidableInstances, DataKinds, DefaultSignatures #-}
-- |
-- Module: Database.PostgreSQL.Typed.Dynamic
-- Copyright: 2015 Dylan Simon
-- 
-- Automatic (dynamic) marshalling of PostgreSQL values based on Haskell types (not SQL statements).
-- This is intended for direct construction of queries and query data, bypassing the normal SQL type inference.

module Database.PostgreSQL.Typed.Dynamic where

import Data.Int
#ifdef USE_SCIENTIFIC
import Data.Scientific (Scientific)
#endif
#ifdef USE_TEXT
import qualified Data.Text as T
#endif
import qualified Data.Time as Time
#ifdef USE_UUID
import qualified Data.UUID as UUID
#endif
import GHC.TypeLits (KnownSymbol)

import Database.PostgreSQL.Typed.Types

-- |Represents canonical/default PostgreSQL representation for various Haskell types, allowing convenient type-driven marshalling.
class KnownSymbol t => PGRep t a | a -> t where
  pgTypeOf :: a -> PGTypeName t
  pgTypeOf _ = PGTypeProxy
  pgEncodeRep :: a -> PGValue
#ifdef USE_BINARY_NEED_ENV
  default pgEncodeRep :: PGBinaryParameter t a => a -> PGValue
  pgEncodeRep x = PGBinaryValue $ pgEncodeBinary (pgTypeOf x) x
#else
  default pgEncodeRep :: PGParameter t a => a -> PGValue
  pgEncodeRep x = PGTextValue $ pgEncode (pgTypeOf x) x
#endif
  pgLiteralRep :: a -> String
  default pgLiteralRep :: PGParameter t a => a -> String
  pgLiteralRep x = pgLiteral (pgTypeOf x) x
  pgDecodeRep :: PGValue -> a
#ifdef USE_BINARY_NEED_ENV
  default pgDecodeRep :: PGBinaryColumn t a => PGValue -> a
  pgDecodeRep (PGBinaryValue v) = pgDecodeBinary (PGTypeProxy :: PGTypeName t) v
#else
  default pgDecodeRep :: PGColumn t a => PGValue -> a
#endif
  pgDecodeRep (PGTextValue v) = pgDecode (PGTypeProxy :: PGTypeName t) v
  pgDecodeRep _ = error $ "pgDecodeRep " ++ pgTypeName (PGTypeProxy :: PGTypeName t) ++ ": unsupported PGValue"

-- |Produce a safely type-cast literal value for interpolation in a SQL statement.
pgSafeLiteral :: PGRep t a => a -> String
pgSafeLiteral x = pgLiteralRep x ++ "::" ++ pgTypeName (pgTypeOf x)

instance PGRep t a => PGRep t (Maybe a) where
  pgEncodeRep Nothing = PGNullValue
  pgEncodeRep (Just x) = pgEncodeRep x
  pgLiteralRep Nothing = "NULL"
  pgLiteralRep (Just x) = pgLiteralRep x
  pgDecodeRep PGNullValue = Nothing
  pgDecodeRep v = Just (pgDecodeRep v)

instance PGRep "boolean" Bool
instance PGRep "oid" OID
instance PGRep "smallint" Int16
instance PGRep "integer" Int32
instance PGRep "bigint" Int64
instance PGRep "real" Float
instance PGRep "double precision" Double
instance PGRep "\"char\"" Char
instance PGRep "text" String
#ifdef USE_TEXT
instance PGRep "text" T.Text
#endif
instance PGRep "date" Time.Day
instance PGRep "time without time zone" Time.TimeOfDay
instance PGRep "timestamp without time zone" Time.LocalTime
instance PGRep "timestamp with time zone" Time.UTCTime
instance PGRep "interval" Time.DiffTime
instance PGRep "numeric" Rational
#ifdef USE_SCIENTIFIC
instance PGRep "numeric" Scientific
#endif
#ifdef USE_UUID
instance PGRep "uuid" UUID.UUID
#endif
