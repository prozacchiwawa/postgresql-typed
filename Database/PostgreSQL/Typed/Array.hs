{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, FunctionalDependencies, UndecidableInstances, DataKinds #-}
-- |
-- Module: Database.PostgreSQL.Typed.Array
-- Copyright: 2015 Dylan Simon
-- 
-- Representaion of PostgreSQL's array type.
-- Currently this only supports one-dimensional arrays.
-- PostgreSQL arrays in theory can dynamically be any (rectangular) shape.

module Database.PostgreSQL.Typed.Array where

import Control.Applicative ((<$>), (<$))
import qualified Data.ByteString.Builder as BSB
import qualified Data.ByteString.Char8 as BSC
import Data.List (intersperse)
import Data.Monoid ((<>), mconcat)
import GHC.TypeLits (KnownSymbol)
import qualified Text.Parsec as P

import Database.PostgreSQL.Typed.Types

-- |The cannonical representation of a PostgreSQL array of any type, which may always contain NULLs.
-- Currenly only one-dimetional arrays are supported, although in PostgreSQL, any array may be of any dimentionality.
type PGArray a = [Maybe a]

-- |Class indicating that the first PostgreSQL type is an array of the second.
-- This implies 'PGParameter' and 'PGColumn' instances that will work for any type using comma as a delimiter (i.e., anything but @box@).
-- This will only work with 1-dimensional arrays.
class (KnownSymbol ta, KnownSymbol t) => PGArrayType ta t | ta -> t, t -> ta where
  pgArrayElementType :: PGTypeName ta -> PGTypeName t
  pgArrayElementType PGTypeProxy = PGTypeProxy
  -- |The character used as a delimeter.  The default @,@ is correct for all standard types (except @box@).
  pgArrayDelim :: PGTypeName ta -> Char
  pgArrayDelim _ = ','

instance (PGArrayType ta t, PGParameter t a) => PGParameter ta (PGArray a) where
  pgEncode ta l = buildPGValue $ BSB.char7 '{' <> mconcat (intersperse (BSB.char7 $ pgArrayDelim ta) $ map el l) <> BSB.char7 '}' where
    el Nothing = BSB.string7 "null"
    el (Just e) = pgDQuote (pgArrayDelim ta : "{}") $ pgEncode (pgArrayElementType ta) e
instance (PGArrayType ta t, PGColumn t a) => PGColumn ta (PGArray a) where
  pgDecode ta = either (error . ("pgDecode array: " ++) . show) id . P.parse pa "array" where
    pa = do
      l <- P.between (P.char '{') (P.char '}') $
        P.sepBy nel (P.char (pgArrayDelim ta))
      _ <- P.eof
      return l
    nel = P.between P.spaces P.spaces $ Nothing <$ nul P.<|> Just <$> el
    nul = P.oneOf "Nn" >> P.oneOf "Uu" >> P.oneOf "Ll" >> P.oneOf "Ll"
    el = pgDecode (pgArrayElementType ta) . BSC.pack <$> parsePGDQuote (pgArrayDelim ta : "{}")

-- Just a dump of pg_type:
instance PGArrayType "boolean[]"       "boolean"
instance PGArrayType "bytea[]"         "bytea"
instance PGArrayType "\"char\"[]"      "\"char\""
instance PGArrayType "name[]"          "name"
instance PGArrayType "bigint[]"        "bigint"
instance PGArrayType "smallint[]"      "smallint"
instance PGArrayType "int2vector[]"    "int2vector"
instance PGArrayType "integer[]"       "integer"
instance PGArrayType "regproc[]"       "regproc"
instance PGArrayType "text[]"          "text"
instance PGArrayType "oid[]"           "oid"
instance PGArrayType "tid[]"           "tid"
instance PGArrayType "xid[]"           "xid"
instance PGArrayType "cid[]"           "cid"
instance PGArrayType "oidvector[]"     "oidvector"
instance PGArrayType "json[]"          "json"
instance PGArrayType "xml[]"           "xml"
instance PGArrayType "point[]"         "point"
instance PGArrayType "lseg[]"          "lseg"
instance PGArrayType "path[]"          "path"
instance PGArrayType "box[]"           "box" where
  pgArrayDelim _ = ';'
instance PGArrayType "polygon[]"       "polygon"
instance PGArrayType "line[]"          "line"
instance PGArrayType "cidr[]"          "cidr"
instance PGArrayType "real[]"          "real"
instance PGArrayType "double precision[]"            "double precision"
instance PGArrayType "abstime[]"       "abstime"
instance PGArrayType "reltime[]"       "reltime"
instance PGArrayType "tinterval[]"     "tinterval"
instance PGArrayType "circle[]"        "circle"
instance PGArrayType "money[]"         "money"
instance PGArrayType "macaddr[]"       "macaddr"
instance PGArrayType "inet[]"          "inet"
instance PGArrayType "aclitem[]"       "aclitem"
instance PGArrayType "bpchar[]"        "bpchar"
instance PGArrayType "character varying[]"           "character varying"
instance PGArrayType "date[]"          "date"
instance PGArrayType "time without time zone[]"      "time without time zone"
instance PGArrayType "timestamp without time zone[]" "timestamp without time zone"
instance PGArrayType "timestamp with time zone[]"    "timestamp with time zone"
instance PGArrayType "interval[]"      "interval"
instance PGArrayType "time with time zone[]"         "time with time zone"
instance PGArrayType "bit[]"           "bit"
instance PGArrayType "varbit[]"        "varbit"
instance PGArrayType "numeric[]"       "numeric"
instance PGArrayType "refcursor[]"     "refcursor"
instance PGArrayType "regprocedure[]"  "regprocedure"
instance PGArrayType "regoper[]"       "regoper"
instance PGArrayType "regoperator[]"   "regoperator"
instance PGArrayType "regclass[]"      "regclass"
instance PGArrayType "regtype[]"       "regtype"
instance PGArrayType "record[]"        "record"
instance PGArrayType "cstring[]"       "cstring"
instance PGArrayType "uuid[]"          "uuid"
instance PGArrayType "txid_snapshot[]" "txid_snapshot"
instance PGArrayType "tsvector[]"      "tsvector"
instance PGArrayType "tsquery[]"       "tsquery"
instance PGArrayType "gtsvector[]"     "gtsvector"
instance PGArrayType "regconfig[]"     "regconfig"
instance PGArrayType "regdictionary[]" "regdictionary"
instance PGArrayType "int4range[]"     "int4range"
instance PGArrayType "numrange[]"      "numrange"
instance PGArrayType "tsrange[]"       "tsrange"
instance PGArrayType "tstzrange[]"     "tstzrange"
instance PGArrayType "daterange[]"     "daterange"
instance PGArrayType "int8range[]"     "int8range"


