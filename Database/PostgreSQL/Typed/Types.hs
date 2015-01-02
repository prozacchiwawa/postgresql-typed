{-# LANGUAGE FlexibleInstances, OverlappingInstances, ScopedTypeVariables, MultiParamTypeClasses, FunctionalDependencies, FlexibleContexts, DataKinds, KindSignatures, TypeFamilies, UndecidableInstances #-}
-- |
-- Module: Database.PostgreSQL.Typed.Type
-- Copyright: 2010, 2011, 2013 Chris Forno
-- Copyright: 2015 Dylan Simon
-- 
-- Classes to support type inference, value encoding/decoding, and instances to support built-in PostgreSQL types.

module Database.PostgreSQL.Typed.Types 
  (
  -- * Basic types
    OID
  , PGValue
  , PGValues
  , pgQuote
  , PGTypeName(..)

  -- * Marshalling classes
  , PGParameter(..)
  , PGColumn(..)
  , PGStringType

  -- * Marshalling utilities
  , pgEncodeParameter
  , pgEscapeParameter
  , pgDecodeColumn
  , pgDecodeColumnNotNull

  -- * Specific type support
  , pgBoolType
  , pgOIDType
  , pgNameType
  , PGArrayType
  , PGRangeType
  ) where

import Control.Applicative ((<$>), (<$))
import Control.Monad (mzero)
import Data.Bits (shiftL, shiftR, (.|.), (.&.))
import Data.ByteString.Internal (w2c)
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.Char8 as LC
import qualified Data.ByteString.Lazy.UTF8 as U
import Data.Char (isDigit, digitToInt, intToDigit, toLower)
import Data.Int
import Data.List (intercalate)
import Data.Ratio ((%), numerator, denominator)
import qualified Data.Time as Time
import Data.Word (Word32)
import GHC.TypeLits (Symbol, symbolVal, KnownSymbol)
import Numeric (readFloat)
import System.Locale (defaultTimeLocale)
import qualified Text.Parsec as P
import Text.Parsec.Token (naturalOrFloat, makeTokenParser, GenLanguageDef(..))

import qualified Database.PostgreSQL.Typed.Range as Range

type PGValue = L.ByteString
-- |A list of (nullable) data values, e.g. a single row or query parameters.
type PGValues = [Maybe PGValue]

-- |A proxy type for PostgreSQL types.  The type argument should be an (internal) name of a database type (see @\\dT+@).
data PGTypeName (t :: Symbol) = PGTypeProxy

pgTypeName :: KnownSymbol t => PGTypeName (t :: Symbol) -> String
pgTypeName = symbolVal

-- |A @PGParameter t a@ instance describes how to encode a PostgreSQL type @t@ from @a@.
class KnownSymbol t => PGParameter (t :: Symbol) a where
  -- |Encode a value to a PostgreSQL text representation.
  pgEncode :: PGTypeName t -> a -> PGValue
  -- |Encode a value to a (quoted) literal value for use in SQL statements.
  -- Defaults to a quoted version of 'pgEncode'
  pgLiteral :: PGTypeName t -> a -> String
  pgLiteral t = pgQuote . U.toString . pgEncode t

-- |A @PGColumn t a@ instance describes how te decode a PostgreSQL type @t@ to @a@.
class KnownSymbol t => PGColumn (t :: Symbol) a where
  -- |Decode the PostgreSQL text representation into a value.
  pgDecode :: PGTypeName t -> PGValue -> a

-- |Support encoding of 'Maybe' values into NULL.
class PGParameterNull t a where
  pgEncodeNull :: PGTypeName t -> a -> Maybe PGValue
  pgLiteralNull :: PGTypeName t -> a -> String

-- |Support decoding of assumed non-null columns but also still allow decoding into 'Maybe'.
class PGColumnNotNull t a where
  pgDecodeNotNull :: PGTypeName t -> Maybe PGValue -> a


instance PGParameter t a => PGParameterNull t a where
  pgEncodeNull t = Just . pgEncode t
  pgLiteralNull = pgLiteral
instance PGParameter t a => PGParameterNull t (Maybe a) where
  pgEncodeNull = fmap . pgEncode
  pgLiteralNull = maybe "NULL" . pgLiteral

instance PGColumn t a => PGColumnNotNull t a where
  pgDecodeNotNull t = maybe (error $ "Unexpected NULL in " ++ pgTypeName t ++ " column") (pgDecode t)
instance PGColumn t a => PGColumnNotNull t (Maybe a) where
  pgDecodeNotNull = fmap . pgDecode

pgEncodeParameter :: PGParameterNull t a => PGTypeName t -> a -> Maybe PGValue
pgEncodeParameter = pgEncodeNull

pgEscapeParameter :: PGParameterNull t a => PGTypeName t -> a -> String
pgEscapeParameter = pgLiteralNull

pgDecodeColumn :: PGColumn t a => PGTypeName t -> Maybe PGValue -> Maybe a
pgDecodeColumn = fmap . pgDecode

pgDecodeColumnNotNull :: PGColumnNotNull t a => PGTypeName t -> Maybe PGValue -> a
pgDecodeColumnNotNull = pgDecodeNotNull


pgQuoteUnsafe :: String -> String
pgQuoteUnsafe s = '\'' : s ++ "'"

-- |Produce a SQL string literal by wrapping (and escaping) a string with single quotes.
pgQuote :: String -> String
pgQuote = ('\'':) . es where
  es "" = "'"
  es (c@'\'':r) = c:c:es r
  es (c:r) = c:es r

dQuote :: String -> String -> String
dQuote _ "" = "\"\""
dQuote unsafe s
  | all (`notElem` unsafe) s && map toLower s /= "null" = s
  | otherwise = '"':es s where
    es "" = "\""
    es (c@'"':r) = '\\':c:es r
    es (c@'\\':r) = '\\':c:es r
    es (c:r) = c:es r

parseDQuote :: P.Stream s m Char => String -> P.ParsecT s u m String
parseDQuote unsafe = (q P.<|> uq) where
  q = P.between (P.char '"') (P.char '"') $
    P.many $ (P.char '\\' >> P.anyChar) P.<|> P.noneOf "\\\""
  uq = P.many1 (P.noneOf unsafe)


class (Show a, Read a, KnownSymbol t) => PGLiteralType t a

instance PGLiteralType t a => PGParameter t a where
  pgEncode _ = LC.pack . show
  pgLiteral _ = show
instance PGLiteralType t a => PGColumn t a where
  pgDecode _ = read . LC.unpack

instance PGParameter "bool" Bool where
  pgEncode _ False = LC.singleton 'f'
  pgEncode _ True = LC.singleton 't'
  pgLiteral _ False = "false"
  pgLiteral _ True = "true"
instance PGColumn "bool" Bool where
  pgDecode _ s = case LC.head s of
    'f' -> False
    't' -> True
    c -> error $ "pgDecode bool: " ++ [c]
pgBoolType :: PGTypeName "bool"
pgBoolType = PGTypeProxy

type OID = Word32
instance PGLiteralType "oid" OID
pgOIDType :: PGTypeName "oid"
pgOIDType = PGTypeProxy

instance PGLiteralType "int2" Int16
instance PGLiteralType "int4" Int32
instance PGLiteralType "int8" Int64
instance PGLiteralType "float4" Float
instance PGLiteralType "float8" Double


instance PGParameter "char" Char where
  pgEncode _ = LC.singleton
instance PGColumn "char" Char where
  pgDecode _ = LC.head


class KnownSymbol t => PGStringType t

instance PGStringType t => PGParameter t String where
  pgEncode _ = U.fromString
instance PGStringType t => PGColumn t String where
  pgDecode _ = U.toString

instance PGStringType t => PGParameter t L.ByteString where
  pgEncode _ = id
instance PGStringType t => PGColumn t L.ByteString where
  pgDecode _ = id

instance PGStringType "text"
instance PGStringType "varchar"
instance PGStringType "name" -- limit 63 characters
pgNameType :: PGTypeName "name"
pgNameType = PGTypeProxy
instance PGStringType "bpchar" -- blank padded


type Bytea = L.ByteString
instance PGParameter "bytea" Bytea where
  pgEncode _ = LC.pack . (++) "'\\x" . ed . L.unpack where
    ed [] = "\'"
    ed (x:d) = hex (shiftR x 4) : hex (x .&. 0xF) : ed d
    hex = intToDigit . fromIntegral
  pgLiteral t = pgQuoteUnsafe . LC.unpack . pgEncode t
instance PGColumn "bytea" Bytea where
  pgDecode _ s
    | sm /= "\\x" = error $ "pgDecode bytea: " ++ sm
    | otherwise = L.pack $ pd $ L.unpack d where
    (m, d) = L.splitAt 2 s
    sm = LC.unpack m
    pd [] = []
    pd (h:l:r) = (shiftL (unhex h) 4 .|. unhex l) : pd r
    pd [x] = error $ "pgDecode bytea: " ++ show x
    unhex = fromIntegral . digitToInt . w2c

instance PGParameter "date" Time.Day where
  pgEncode _ = LC.pack . Time.showGregorian
  pgLiteral t = pgQuoteUnsafe . LC.unpack . pgEncode t
instance PGColumn "date" Time.Day where
  pgDecode _ = Time.readTime defaultTimeLocale "%F" . LC.unpack

instance PGParameter "time" Time.TimeOfDay where
  pgEncode _ = LC.pack . Time.formatTime defaultTimeLocale "%T%Q"
  pgLiteral t = pgQuoteUnsafe . LC.unpack . pgEncode t
instance PGColumn "time" Time.TimeOfDay where
  pgDecode _ = Time.readTime defaultTimeLocale "%T%Q" . LC.unpack

instance PGParameter "timestamp" Time.LocalTime where
  pgEncode _ = LC.pack . Time.formatTime defaultTimeLocale "%F %T%Q"
  pgLiteral t = pgQuoteUnsafe . LC.unpack . pgEncode t
instance PGColumn "timestamp" Time.LocalTime where
  pgDecode _ = Time.readTime defaultTimeLocale "%F %T%Q" . LC.unpack

-- PostgreSQL uses "[+-]HH[:MM]" timezone offsets, while "%z" uses "+HHMM" by default.
-- readTime can successfully parse both formats, but PostgreSQL needs the colon.
fixTZ :: String -> String
fixTZ "" = ""
fixTZ ['+',h1,h2] | isDigit h1 && isDigit h2 = ['+',h1,h2,':','0','0']
fixTZ ['-',h1,h2] | isDigit h1 && isDigit h2 = ['-',h1,h2,':','0','0']
fixTZ ['+',h1,h2,m1,m2] | isDigit h1 && isDigit h2 && isDigit m1 && isDigit m2 = ['+',h1,h2,':',m1,m2]
fixTZ ['-',h1,h2,m1,m2] | isDigit h1 && isDigit h2 && isDigit m1 && isDigit m2 = ['-',h1,h2,':',m1,m2]
fixTZ (c:s) = c:fixTZ s

instance PGParameter "timestamptz" Time.UTCTime where
  pgEncode _ = LC.pack . fixTZ . Time.formatTime defaultTimeLocale "%F %T%Q%z"
  -- pgLiteral t = pgQuoteUnsafe . LC.unpack . pgEncode t
instance PGColumn "timestamptz" Time.UTCTime where
  pgDecode _ = Time.readTime defaultTimeLocale "%F %T%Q%z" . fixTZ . LC.unpack

instance PGParameter "interval" Time.DiffTime where
  pgEncode _ = LC.pack . show
  pgLiteral _ = pgQuoteUnsafe . show
-- |Representation of DiffTime as interval.
-- PostgreSQL stores months and days separately in intervals, but DiffTime does not.
-- We collapse all interval fields into seconds
instance PGColumn "interval" Time.DiffTime where
  pgDecode _ = either (error . ("pgDecode interval: " ++) . show) id . P.parse ps "interval" where
    ps = do
      _ <- P.char 'P'
      d <- units [('Y', 12*month), ('M', month), ('W', 7*day), ('D', day)]
      (d +) <$> pt P.<|> d <$ P.eof
    pt = do
      _ <- P.char 'T'
      t <- units [('H', 3600), ('M', 60), ('S', 1)]
      _ <- P.eof
      return t
    units l = fmap sum $ P.many $ do
      s <- negate <$ P.char '-' P.<|> id <$ P.char '+' P.<|> return id
      x <- num
      u <- P.choice $ map (\(c, u) -> s u <$ P.char c) l
      return $ either (Time.secondsToDiffTime . (* u)) (realToFrac . (* fromInteger u)) x
    day = 86400
    month = 2629746
    num = naturalOrFloat $ makeTokenParser $ LanguageDef
      { commentStart   = ""
      , commentEnd     = ""
      , commentLine    = ""
      , nestedComments = False
      , identStart     = mzero
      , identLetter    = mzero
      , opStart        = mzero
      , opLetter       = mzero
      , reservedOpNames= []
      , reservedNames  = []
      , caseSensitive  = True
      }

instance PGParameter "numeric" Rational where
  pgEncode _ r
    | denominator r == 0 = LC.pack "NaN" -- this can't happen
    | otherwise = LC.pack $ take 30 (showRational (r / (10 ^^ e))) ++ 'e' : show e where
    e = floor $ logBase (10 :: Double) $ fromRational $ abs r :: Int -- not great, and arbitrarily truncate somewhere
  pgLiteral _ r
    | denominator r == 0 = "'NaN'" -- this can't happen
    | otherwise = '(' : show (numerator r) ++ '/' : show (denominator r) ++ "::numeric)"
-- |High-precision representation of Rational as numeric.
-- Unfortunately, numeric has an NaN, while Rational does not.
-- NaN numeric values will produce exceptions.
instance PGColumn "numeric" Rational where
  pgDecode _ bs
    | s == "NaN" = 0 % 0 -- this won't work
    | otherwise = ur $ readFloat s where
    ur [(x,"")] = x
    ur _ = error $ "pgDecode numeric: " ++ s
    s = LC.unpack bs

-- This will produce infinite(-precision) strings
showRational :: Rational -> String
showRational r = show (ri :: Integer) ++ '.' : frac (abs rf) where
  (ri, rf) = properFraction r
  frac 0 = ""
  frac f = intToDigit i : frac f' where (i, f') = properFraction (10 * f)

-- |The cannonical representation of a PostgreSQL array of any type, which may always contain NULLs.
type PGArray a = [Maybe a]

-- |Class indicating that the first PostgreSQL type is an array of the second.
-- This implies 'PGParameter' and 'PGColumn" instances that will work for any type using comma as a delimiter (i.e., anything but @box@).
class (KnownSymbol ta, KnownSymbol t) => PGArrayType ta t | ta -> t, t -> ta where
  pgArrayElementType :: PGTypeName ta -> PGTypeName t
  pgArrayElementType PGTypeProxy = PGTypeProxy
  -- |The character used as a delimeter.  The default @,@ is correct for all standard types (except @box@).
  pgArrayDelim :: PGTypeName ta -> Char
  pgArrayDelim _ = ','

instance (PGArrayType ta t, PGParameter t a) => PGParameter ta (PGArray a) where
  -- TODO: rewrite to use BS
  pgEncode ta l = U.fromString $ '{' : intercalate [pgArrayDelim ta] (map el l) ++ "}" where
    el Nothing = "null"
    el (Just e) = dQuote (pgArrayDelim ta : "\"\\{}") $ U.toString $ pgEncode (pgArrayElementType ta) e
instance (PGArrayType ta t, PGColumn t a) => PGColumn ta (PGArray a) where
  pgDecode ta = either (error . ("pgDecode array: " ++) . show) id . P.parse pa "array" where
    pa = do
      l <- P.between (P.char '{') (P.char '}') $
        P.sepBy nel (P.char (pgArrayDelim ta))
      _ <- P.eof
      return l
    nel = Nothing <$ nul P.<|> Just <$> el
    nul = P.oneOf "Nn" >> P.oneOf "Uu" >> P.oneOf "Ll" >> P.oneOf "Ll"
    el = pgDecode (pgArrayElementType ta) . LC.pack <$> parseDQuote (pgArrayDelim ta : "\"{}")

-- Just a dump of pg_type:
instance PGArrayType "_bool"          "bool"
instance PGArrayType "_bytea"         "bytea"
instance PGArrayType "_char"          "char"
instance PGArrayType "_name"          "name"
instance PGArrayType "_int8"          "int8"
instance PGArrayType "_int2"          "int2"
instance PGArrayType "_int2vector"    "int2vector"
instance PGArrayType "_int4"          "int4"
instance PGArrayType "_regproc"       "regproc"
instance PGArrayType "_text"          "text"
instance PGArrayType "_oid"           "oid"
instance PGArrayType "_tid"           "tid"
instance PGArrayType "_xid"           "xid"
instance PGArrayType "_cid"           "cid"
instance PGArrayType "_oidvector"     "oidvector"
instance PGArrayType "_json"          "json"
instance PGArrayType "_xml"           "xml"
instance PGArrayType "_point"         "point"
instance PGArrayType "_lseg"          "lseg"
instance PGArrayType "_path"          "path"
instance PGArrayType "_box"           "box" where
  pgArrayDelim _ = ';'
instance PGArrayType "_polygon"       "polygon"
instance PGArrayType "_line"          "line"
instance PGArrayType "_cidr"          "cidr"
instance PGArrayType "_float4"        "float4"
instance PGArrayType "_float8"        "float8"
instance PGArrayType "_abstime"       "abstime"
instance PGArrayType "_reltime"       "reltime"
instance PGArrayType "_tinterval"     "tinterval"
instance PGArrayType "_circle"        "circle"
instance PGArrayType "_money"         "money"
instance PGArrayType "_macaddr"       "macaddr"
instance PGArrayType "_inet"          "inet"
instance PGArrayType "_aclitem"       "aclitem"
instance PGArrayType "_bpchar"        "bpchar"
instance PGArrayType "_varchar"       "varchar"
instance PGArrayType "_date"          "date"
instance PGArrayType "_time"          "time"
instance PGArrayType "_timestamp"     "timestamp"
instance PGArrayType "_timestamptz"   "timestamptz"
instance PGArrayType "_interval"      "interval"
instance PGArrayType "_timetz"        "timetz"
instance PGArrayType "_bit"           "bit"
instance PGArrayType "_varbit"        "varbit"
instance PGArrayType "_numeric"       "numeric"
instance PGArrayType "_refcursor"     "refcursor"
instance PGArrayType "_regprocedure"  "regprocedure"
instance PGArrayType "_regoper"       "regoper"
instance PGArrayType "_regoperator"   "regoperator"
instance PGArrayType "_regclass"      "regclass"
instance PGArrayType "_regtype"       "regtype"
instance PGArrayType "_record"        "record"
instance PGArrayType "_cstring"       "cstring"
instance PGArrayType "_uuid"          "uuid"
instance PGArrayType "_txid_snapshot" "txid_snapshot"
instance PGArrayType "_tsvector"      "tsvector"
instance PGArrayType "_tsquery"       "tsquery"
instance PGArrayType "_gtsvector"     "gtsvector"
instance PGArrayType "_regconfig"     "regconfig"
instance PGArrayType "_regdictionary" "regdictionary"
instance PGArrayType "_int4range"     "int4range"
instance PGArrayType "_numrange"      "numrange"
instance PGArrayType "_tsrange"       "tsrange"
instance PGArrayType "_tstzrange"     "tstzrange"
instance PGArrayType "_daterange"     "daterange"
instance PGArrayType "_int8range"     "int8range"


-- |Class indicating that the first PostgreSQL type is a range of the second.
-- This implies 'PGParameter' and 'PGColumn" instances that will work for any type.
class (KnownSymbol tr, KnownSymbol t) => PGRangeType tr t | tr -> t where
  pgRangeElementType :: PGTypeName tr -> PGTypeName t
  pgRangeElementType PGTypeProxy = PGTypeProxy

instance (PGRangeType tr t, PGParameter t a) => PGParameter tr (Range.Range a) where
  pgEncode _ Range.Empty = LC.pack "empty"
  -- TODO: rewrite to use BS
  pgEncode tr (Range.Range (Range.Lower l) (Range.Upper u)) = U.fromString $
    pc '[' '(' l
      : pb (Range.bound l)
      ++ ','
      : pb (Range.bound u)
      ++ [pc ']' ')' u]
    where
    pb Nothing = ""
    pb (Just b) = dQuote "\"(),[\\]" $ U.toString $ pgEncode (pgRangeElementType tr) b
    pc c o b = if Range.boundClosed b then c else o
instance (PGRangeType tr t, PGColumn t a) => PGColumn tr (Range.Range a) where
  pgDecode tr = either (error . ("pgDecode range: " ++) . show) id . P.parse per "array" where
    per = Range.Empty <$ pe P.<|> pr
    pe = P.oneOf "Ee" >> P.oneOf "Mm" >> P.oneOf "Pp" >> P.oneOf "Tt" >> P.oneOf "Yy"
    pp = pgDecode (pgRangeElementType tr) . LC.pack <$> parseDQuote "\"(),[\\]"
    pc c o = True <$ P.char c P.<|> False <$ P.char o
    pb = P.optionMaybe pp
    mb = maybe Range.Unbounded . Range.Bounded
    pr = do
      lc <- pc '[' '('
      lb <- pb
      _ <- P.char ','
      ub <- pb 
      uc <- pc ']' ')'
      return $ Range.Range (Range.Lower (mb lc lb)) (Range.Upper (mb uc ub))

instance PGRangeType "int4range" "int4"
instance PGRangeType "numrange" "numeric"
instance PGRangeType "tsrange" "timestamp"
instance PGRangeType "tstzrange" "timestamptz"
instance PGRangeType "daterange" "date"
instance PGRangeType "int8range" "int8"


{-
--, ( 114,  199, "json",        ?)
--, ( 142,  143, "xml",         ?)
--, ( 600, 1017, "point",       ?)
--, ( 650,  651, "cidr",        ?)
--, ( 790,  791, "money",       Centi? Fixed?)
--, ( 829, 1040, "macaddr",     ?)
--, ( 869, 1041, "inet",        ?)
--, (1266, 1270, "timetz",      ?)
--, (1560, 1561, "bit",         Bool?)
--, (1562, 1563, "varbit",      ?)
--, (2950, 2951, "uuid",        ?)
-}
