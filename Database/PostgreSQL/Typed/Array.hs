{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, FunctionalDependencies, UndecidableInstances, DataKinds #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
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
import qualified Text.Parsec as P

import Database.PostgreSQL.Typed.Types

-- |The cannonical representation of a PostgreSQL array of any type, which may always contain NULLs.
-- Currenly only one-dimetional arrays are supported, although in PostgreSQL, any array may be of any dimentionality.
type PGArray a = [Maybe a]

-- |Class indicating that the first PostgreSQL type is an array of the second.
-- This implies 'PGParameter' and 'PGColumn' instances that will work for any type using comma as a delimiter (i.e., anything but @box@).
-- This will only work with 1-dimensional arrays.
class (PGType ta, PGType t) => PGArrayType ta t | ta -> t, t -> ta where
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
  pgDecode ta a = either (error . ("pgDecode array: " ++) . show) id $ P.parse pa (BSC.unpack a) a where
    pa = do
      l <- P.between (P.char '{') (P.char '}') $
        P.sepBy nel (P.char (pgArrayDelim ta))
      _ <- P.eof
      return l
    nel = P.between P.spaces P.spaces $ Nothing <$ nul P.<|> Just <$> el
    nul = P.oneOf "Nn" >> P.oneOf "Uu" >> P.oneOf "Ll" >> P.oneOf "Ll"
    el = pgDecode (pgArrayElementType ta) . BSC.pack <$> parsePGDQuote (pgArrayDelim ta : "{}")

-- Just a dump of pg_type:
instance PGType "boolean" => PGType "boolean[]"
instance PGType "boolean" => PGArrayType "boolean[]" "boolean"
instance PGType "bytea" => PGType "bytea[]"
instance PGType "bytea" => PGArrayType "bytea[]" "bytea"
instance PGType "\"char\"" => PGType "\"char\"[]"
instance PGType "\"char\"" => PGArrayType "\"char\"[]" "\"char\""
instance PGType "name" => PGType "name[]"
instance PGType "name" => PGArrayType "name[]" "name"
instance PGType "bigint" => PGType "bigint[]"
instance PGType "bigint" => PGArrayType "bigint[]" "bigint"
instance PGType "smallint" => PGType "smallint[]"
instance PGType "smallint" => PGArrayType "smallint[]" "smallint"
instance PGType "int2vector" => PGType "int2vector[]"
instance PGType "int2vector" => PGArrayType "int2vector[]" "int2vector"
instance PGType "integer" => PGType "integer[]"
instance PGType "integer" => PGArrayType "integer[]" "integer"
instance PGType "regproc" => PGType "regproc[]"
instance PGType "regproc" => PGArrayType "regproc[]" "regproc"
instance PGType "text" => PGType "text[]"
instance PGType "text" => PGArrayType "text[]" "text"
instance PGType "oid" => PGType "oid[]"
instance PGType "oid" => PGArrayType "oid[]" "oid"
instance PGType "tid" => PGType "tid[]"
instance PGType "tid" => PGArrayType "tid[]" "tid"
instance PGType "xid" => PGType "xid[]"
instance PGType "xid" => PGArrayType "xid[]" "xid"
instance PGType "cid" => PGType "cid[]"
instance PGType "cid" => PGArrayType "cid[]" "cid"
instance PGType "oidvector" => PGType "oidvector[]"
instance PGType "oidvector" => PGArrayType "oidvector[]" "oidvector"
instance PGType "json" => PGType "json[]"
instance PGType "json" => PGArrayType "json[]" "json"
instance PGType "xml" => PGType "xml[]"
instance PGType "xml" => PGArrayType "xml[]" "xml"
instance PGType "point" => PGType "point[]"
instance PGType "point" => PGArrayType "point[]" "point"
instance PGType "lseg" => PGType "lseg[]"
instance PGType "lseg" => PGArrayType "lseg[]" "lseg"
instance PGType "path" => PGType "path[]"
instance PGType "path" => PGArrayType "path[]" "path"
instance PGType "box" => PGType "box[]"
instance PGType "box" => PGArrayType "box[]" "box" where
  pgArrayDelim _ = ';'
instance PGType "polygon" => PGType "polygon[]"
instance PGType "polygon" => PGArrayType "polygon[]" "polygon"
instance PGType "line" => PGType "line[]"
instance PGType "line" => PGArrayType "line[]" "line"
instance PGType "cidr" => PGType "cidr[]"
instance PGType "cidr" => PGArrayType "cidr[]" "cidr"
instance PGType "real" => PGType "real[]"
instance PGType "real" => PGArrayType "real[]" "real"
instance PGType "double precision" => PGType "double precision[]"
instance PGType "double precision" => PGArrayType "double precision[]" "double precision"
instance PGType "abstime" => PGType "abstime[]"
instance PGType "abstime" => PGArrayType "abstime[]" "abstime"
instance PGType "reltime" => PGType "reltime[]"
instance PGType "reltime" => PGArrayType "reltime[]" "reltime"
instance PGType "tinterval" => PGType "tinterval[]"
instance PGType "tinterval" => PGArrayType "tinterval[]" "tinterval"
instance PGType "circle" => PGType "circle[]"
instance PGType "circle" => PGArrayType "circle[]" "circle"
instance PGType "money" => PGType "money[]"
instance PGType "money" => PGArrayType "money[]" "money"
instance PGType "macaddr" => PGType "macaddr[]"
instance PGType "macaddr" => PGArrayType "macaddr[]" "macaddr"
instance PGType "inet" => PGType "inet[]"
instance PGType "inet" => PGArrayType "inet[]" "inet"
instance PGType "aclitem" => PGType "aclitem[]"
instance PGType "aclitem" => PGArrayType "aclitem[]" "aclitem"
instance PGType "bpchar" => PGType "bpchar[]"
instance PGType "bpchar" => PGArrayType "bpchar[]" "bpchar"
instance PGType "character varying" => PGType "character varying[]"
instance PGType "character varying" => PGArrayType "character varying[]" "character varying"
instance PGType "date" => PGType "date[]"
instance PGType "date" => PGArrayType "date[]" "date"
instance PGType "time without time zone" => PGType "time without time zone[]"
instance PGType "time without time zone" => PGArrayType "time without time zone[]" "time without time zone"
instance PGType "timestamp without time zone" => PGType "timestamp without time zone[]"
instance PGType "timestamp without time zone" => PGArrayType "timestamp without time zone[]" "timestamp without time zone"
instance PGType "timestamp with time zone" => PGType "timestamp with time zone[]"
instance PGType "timestamp with time zone" => PGArrayType "timestamp with time zone[]" "timestamp with time zone"
instance PGType "interval" => PGType "interval[]"
instance PGType "interval" => PGArrayType "interval[]" "interval"
instance PGType "time with time zone" => PGType "time with time zone[]"
instance PGType "time with time zone" => PGArrayType "time with time zone[]" "time with time zone"
instance PGType "bit" => PGType "bit[]"
instance PGType "bit" => PGArrayType "bit[]" "bit"
instance PGType "varbit" => PGType "varbit[]"
instance PGType "varbit" => PGArrayType "varbit[]" "varbit"
instance PGType "numeric" => PGType "numeric[]"
instance PGType "numeric" => PGArrayType "numeric[]" "numeric"
instance PGType "refcursor" => PGType "refcursor[]"
instance PGType "refcursor" => PGArrayType "refcursor[]" "refcursor"
instance PGType "regprocedure" => PGType "regprocedure[]"
instance PGType "regprocedure" => PGArrayType "regprocedure[]" "regprocedure"
instance PGType "regoper" => PGType "regoper[]"
instance PGType "regoper" => PGArrayType "regoper[]" "regoper"
instance PGType "regoperator" => PGType "regoperator[]"
instance PGType "regoperator" => PGArrayType "regoperator[]" "regoperator"
instance PGType "regclass" => PGType "regclass[]"
instance PGType "regclass" => PGArrayType "regclass[]" "regclass"
instance PGType "regtype" => PGType "regtype[]"
instance PGType "regtype" => PGArrayType "regtype[]" "regtype"
instance PGType "record" => PGType "record[]"
instance PGType "record" => PGArrayType "record[]" "record"
instance PGType "cstring" => PGType "cstring[]"
instance PGType "cstring" => PGArrayType "cstring[]" "cstring"
instance PGType "uuid" => PGType "uuid[]"
instance PGType "uuid" => PGArrayType "uuid[]" "uuid"
instance PGType "txid_snapshot" => PGType "txid_snapshot[]"
instance PGType "txid_snapshot" => PGArrayType "txid_snapshot[]" "txid_snapshot"
instance PGType "tsvector" => PGType "tsvector[]"
instance PGType "tsvector" => PGArrayType "tsvector[]" "tsvector"
instance PGType "tsquery" => PGType "tsquery[]"
instance PGType "tsquery" => PGArrayType "tsquery[]" "tsquery"
instance PGType "gtsvector" => PGType "gtsvector[]"
instance PGType "gtsvector" => PGArrayType "gtsvector[]" "gtsvector"
instance PGType "regconfig" => PGType "regconfig[]"
instance PGType "regconfig" => PGArrayType "regconfig[]" "regconfig"
instance PGType "regdictionary" => PGType "regdictionary[]"
instance PGType "regdictionary" => PGArrayType "regdictionary[]" "regdictionary"
instance PGType "int4range" => PGType "int4range[]"
instance PGType "int4range" => PGArrayType "int4range[]" "int4range"
instance PGType "numrange" => PGType "numrange[]"
instance PGType "numrange" => PGArrayType "numrange[]" "numrange"
instance PGType "tsrange" => PGType "tsrange[]"
instance PGType "tsrange" => PGArrayType "tsrange[]" "tsrange"
instance PGType "tstzrange" => PGType "tstzrange[]"
instance PGType "tstzrange" => PGArrayType "tstzrange[]" "tstzrange"
instance PGType "daterange" => PGType "daterange[]"
instance PGType "daterange" => PGArrayType "daterange[]" "daterange"
instance PGType "int8range" => PGType "int8range[]"
instance PGType "int8range" => PGArrayType "int8range[]" "int8range"

