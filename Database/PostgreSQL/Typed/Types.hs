{-# LANGUAGE CPP, FlexibleInstances, OverlappingInstances, ScopedTypeVariables, MultiParamTypeClasses, FunctionalDependencies, FlexibleContexts, DataKinds, KindSignatures, TypeFamilies, UndecidableInstances #-}
-- |
-- Module: Database.PostgreSQL.Typed.Types
-- Copyright: 2015 Dylan Simon
-- 
-- Classes to support type inference, value encoding/decoding, and instances to support built-in PostgreSQL types.

module Database.PostgreSQL.Typed.Types 
  (
  -- * Basic types
    OID
  , PGValue(..)
  , PGValues
  , pgQuote
  , PGTypeName(..)
  , PGTypeEnv(..)

  -- * Marshalling classes
  , PGParameter(..)
  , PGBinaryParameter
  , PGColumn(..)
  , PGBinaryType

  -- * Marshalling utilities
  , pgEncodeParameter
  , pgEncodeBinaryParameter
  , pgEscapeParameter
  , pgDecodeColumn
  , pgDecodeColumnNotNull
  , pgDecodeBinaryColumn
  , pgDecodeBinaryColumnNotNull

  -- * Specific type support
  , PGArrayType
  , PGRangeType
  ) where

import Control.Applicative ((<$>), (<$))
import Control.Monad (mzero)
import Data.Bits (shiftL, (.|.))
import Data.ByteString.Internal (w2c)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BSB
import qualified Data.ByteString.Builder.Prim as BSBP
import qualified Data.ByteString.Char8 as BSC
import Data.ByteString.Internal (c2w)
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.UTF8 as BSU
import Data.Char (isSpace, isDigit, digitToInt, intToDigit, toLower)
import Data.Int
import Data.List (intersperse)
import Data.Maybe (fromMaybe)
import Data.Monoid ((<>), mconcat, mempty)
import Data.Ratio ((%), numerator, denominator)
#ifdef USE_SCIENTIFIC
import Data.Scientific (Scientific)
#endif
#ifdef USE_TEXT
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TLE
#endif
import qualified Data.Time as Time
#ifdef USE_UUID
import qualified Data.UUID as UUID
#endif
import Data.Word (Word8, Word32)
import GHC.TypeLits (Symbol, symbolVal, KnownSymbol)
import Numeric (readFloat)
#ifdef USE_BINARY
-- import qualified PostgreSQLBinary.Array as BinA
import qualified PostgreSQLBinary.Decoder as BinD
import qualified PostgreSQLBinary.Encoder as BinE
#endif
import System.Locale (defaultTimeLocale)
import qualified Text.Parsec as P
import Text.Parsec.Token (naturalOrFloat, makeTokenParser, GenLanguageDef(..))

import qualified Database.PostgreSQL.Typed.Range as Range

type PGTextValue = BS.ByteString
type PGBinaryValue = BS.ByteString
-- |A value passed to or from PostgreSQL in raw format.
data PGValue
  = PGNullValue
  | PGTextValue PGTextValue -- ^ The standard text encoding format (also used for unknown formats)
  | PGBinaryValue PGBinaryValue -- ^ Special binary-encoded data.  Not supported in all cases.
  deriving (Show, Eq)
-- |A list of (nullable) data values, e.g. a single row or query parameters.
type PGValues = [PGValue]

-- |A proxy type for PostgreSQL types.  The type argument should be an (internal) name of a database type (see @\\dT+@).
data PGTypeName (t :: Symbol) = PGTypeProxy

class KnownSymbol t => PGBinaryType t

pgTypeName :: KnownSymbol t => PGTypeName (t :: Symbol) -> String
pgTypeName = symbolVal

-- |Parameters that affect how marshalling happens.
-- Currenly we force all other relevant parameters at connect time.
data PGTypeEnv = PGTypeEnv
  { pgIntegerDatetimes :: Bool -- ^ If @integer_datetimes@ is @on@; only relevant for binary encoding.
  }

-- |A @PGParameter t a@ instance describes how to encode a PostgreSQL type @t@ from @a@.
class KnownSymbol t => PGParameter (t :: Symbol) a where
  -- |Encode a value to a PostgreSQL text representation.
  pgEncode :: PGTypeName t -> a -> PGTextValue
  -- |Encode a value to a (quoted) literal value for use in SQL statements.
  -- Defaults to a quoted version of 'pgEncode'
  pgLiteral :: PGTypeName t -> a -> String
  pgLiteral t = pgQuote . BSU.toString . pgEncode t
class (PGParameter t a, PGBinaryType t) => PGBinaryParameter t a where
  pgEncodeBinary :: PGTypeEnv -> PGTypeName t -> a -> PGBinaryValue

-- |A @PGColumn t a@ instance describes how te decode a PostgreSQL type @t@ to @a@.
class KnownSymbol t => PGColumn (t :: Symbol) a where
  -- |Decode the PostgreSQL text representation into a value.
  pgDecode :: PGTypeName t -> PGTextValue -> a
class (PGColumn t a, PGBinaryType t) => PGBinaryColumn t a where
  pgDecodeBinary :: PGTypeEnv -> PGTypeName t -> PGBinaryValue -> a

-- |Support encoding of 'Maybe' values into NULL.
class PGParameterNull t a where
  pgEncodeNull :: PGTypeName t -> a -> PGValue
  pgLiteralNull :: PGTypeName t -> a -> String
class PGParameterNull t a => PGBinaryParameterNull t a where
  pgEncodeBinaryNull :: PGTypeEnv -> PGTypeName t -> a -> PGValue

-- |Support decoding of assumed non-null columns but also still allow decoding into 'Maybe'.
class PGColumnNotNull t a where
  pgDecodeNotNull :: PGTypeName t -> PGValue -> a


instance PGParameter t a => PGParameterNull t a where
  pgEncodeNull t = PGTextValue . pgEncode t
  pgLiteralNull = pgLiteral
instance PGParameter t a => PGParameterNull t (Maybe a) where
  pgEncodeNull t = maybe PGNullValue (PGTextValue . pgEncode t)
  pgLiteralNull = maybe "NULL" . pgLiteral
instance PGBinaryParameter t a => PGBinaryParameterNull t a where
  pgEncodeBinaryNull e t = PGBinaryValue . pgEncodeBinary e t
instance PGBinaryParameter t a => PGBinaryParameterNull t (Maybe a) where
  pgEncodeBinaryNull e t = maybe PGNullValue (PGBinaryValue . pgEncodeBinary e t)

instance PGColumn t a => PGColumnNotNull t a where
  pgDecodeNotNull t PGNullValue = error $ "NULL in " ++ pgTypeName t ++ " column (use Maybe or COALESCE)"
  pgDecodeNotNull t (PGTextValue v) = pgDecode t v
  pgDecodeNotNull t (PGBinaryValue _) = error $ "pgDecode: unexpected binary value in " ++ pgTypeName t
instance PGColumn t a => PGColumnNotNull t (Maybe a) where
  pgDecodeNotNull _ PGNullValue = Nothing
  pgDecodeNotNull t (PGTextValue v) = Just $ pgDecode t v
  pgDecodeNotNull t (PGBinaryValue _) = error $ "pgDecode: unexpected binary value in " ++ pgTypeName t


-- |Final parameter encoding function used when a (nullable) parameter is passed to a prepared query.
pgEncodeParameter :: PGParameterNull t a => PGTypeEnv -> PGTypeName t -> a -> PGValue
pgEncodeParameter _ = pgEncodeNull

-- |Final parameter encoding function used when a (nullable) parameter is passed to a prepared query accepting binary-encoded data.
pgEncodeBinaryParameter :: PGBinaryParameterNull t a => PGTypeEnv -> PGTypeName t -> a -> PGValue
pgEncodeBinaryParameter = pgEncodeBinaryNull

-- |Final parameter escaping function used when a (nullable) parameter is passed to be substituted into a simple query.
pgEscapeParameter :: PGParameterNull t a => PGTypeEnv -> PGTypeName t -> a -> String
pgEscapeParameter _ = pgLiteralNull

-- |Final column decoding function used for a nullable result value.
pgDecodeColumn :: PGColumnNotNull t (Maybe a) => PGTypeEnv -> PGTypeName t -> PGValue -> Maybe a
pgDecodeColumn _ = pgDecodeNotNull

-- |Final column decoding function used for a non-nullable result value.
pgDecodeColumnNotNull :: PGColumnNotNull t a => PGTypeEnv -> PGTypeName t -> PGValue -> a
pgDecodeColumnNotNull _ = pgDecodeNotNull

-- |Final column decoding function used for a nullable binary-encoded result value.
pgDecodeBinaryColumn :: PGBinaryColumn t a => PGTypeEnv -> PGTypeName t -> PGValue -> Maybe a
pgDecodeBinaryColumn e t (PGBinaryValue v) = Just $ pgDecodeBinary e t v
pgDecodeBinaryColumn e t v = pgDecodeColumn e t v

-- |Final column decoding function used for a non-nullable binary-encoded result value.
pgDecodeBinaryColumnNotNull :: (PGColumnNotNull t a, PGBinaryColumn t a) => PGTypeEnv -> PGTypeName t -> PGValue -> a
pgDecodeBinaryColumnNotNull e t (PGBinaryValue v) = pgDecodeBinary e t v
pgDecodeBinaryColumnNotNull _ t v = pgDecodeNotNull t v


pgQuoteUnsafe :: String -> String
pgQuoteUnsafe s = '\'' : s ++ "'"

-- |Produce a SQL string literal by wrapping (and escaping) a string with single quotes.
pgQuote :: String -> String
pgQuote = ('\'':) . es where
  es "" = "'"
  es (c@'\'':r) = c:c:es r
  es (c:r) = c:es r

buildBS :: BSB.Builder -> BS.ByteString
buildBS = BSL.toStrict . BSB.toLazyByteString

-- |Double-quote a value if it's \"\", \"null\", or contains any whitespace, \'\"\', \'\\\', or the characters given in the first argument.
-- Checking all these things may not be worth it.  We could just double-quote everything.
dQuote :: String -> BS.ByteString -> BSB.Builder
dQuote unsafe s
  | BS.null s || BSC.any (\c -> isSpace c || c == '"' || c == '\\' || c `elem` unsafe) s || BSC.map toLower s == BSC.pack "null" =
    dq <> BSBP.primMapByteStringBounded ec s <> dq
  | otherwise = BSB.byteString s where
  dq = BSB.char7 '"'
  ec = BSBP.condB (\c -> c == c2w '"' || c == c2w '\\') bs (BSBP.liftFixedToBounded BSBP.word8)
  bs = BSBP.liftFixedToBounded $ ((,) '\\') BSBP.>$< (BSBP.char7 BSBP.>*< BSBP.word8)

parseDQuote :: P.Stream s m Char => String -> P.ParsecT s u m String
parseDQuote unsafe = (q P.<|> uq) where
  q = P.between (P.char '"') (P.char '"') $
    P.many $ (P.char '\\' >> P.anyChar) P.<|> P.noneOf "\\\""
  uq = P.many1 (P.noneOf ('"':'\\':unsafe))


class (Show a, Read a, KnownSymbol t) => PGLiteralType t a

instance PGLiteralType t a => PGParameter t a where
  pgEncode _ = BSC.pack . show
  pgLiteral _ = show
instance PGLiteralType t a => PGColumn t a where
  pgDecode _ = read . BSC.unpack

instance PGParameter "boolean" Bool where
  pgEncode _ False = BSC.singleton 'f'
  pgEncode _ True = BSC.singleton 't'
  pgLiteral _ False = "false"
  pgLiteral _ True = "true"
instance PGColumn "boolean" Bool where
  pgDecode _ s = case BSC.head s of
    'f' -> False
    't' -> True
    c -> error $ "pgDecode boolean: " ++ [c]

type OID = Word32
instance PGLiteralType "oid" OID
instance PGLiteralType "smallint" Int16
instance PGLiteralType "integer" Int32
instance PGLiteralType "bigint" Int64
instance PGLiteralType "real" Float
instance PGLiteralType "double precision" Double


instance PGParameter "\"char\"" Char where
  pgEncode _ = BSC.singleton
instance PGColumn "\"char\"" Char where
  pgDecode _ = BSC.head


class KnownSymbol t => PGStringType t

instance PGStringType t => PGParameter t String where
  pgEncode _ = BSU.fromString
instance PGStringType t => PGColumn t String where
  pgDecode _ = BSU.toString

instance PGStringType t => PGParameter t BS.ByteString where
  pgEncode _ = id
instance PGStringType t => PGColumn t BS.ByteString where
  pgDecode _ = id

instance PGStringType t => PGParameter t BSL.ByteString where
  pgEncode _ = BSL.toStrict
instance PGStringType t => PGColumn t BSL.ByteString where
  pgDecode _ = BSL.fromStrict

#ifdef USE_TEXT
instance PGStringType t => PGParameter t T.Text where
  pgEncode _ = TE.encodeUtf8
instance PGStringType t => PGColumn t T.Text where
  pgDecode _ = TE.decodeUtf8

instance PGStringType t => PGParameter t TL.Text where
  pgEncode _ = BSL.toStrict . TLE.encodeUtf8
instance PGStringType t => PGColumn t TL.Text where
  pgDecode _ = TL.fromStrict . TE.decodeUtf8
#endif

instance PGStringType "text"
instance PGStringType "character varying"
instance PGStringType "name" -- limit 63 characters
instance PGStringType "bpchar" -- blank padded


encodeBytea :: BSB.Builder -> PGTextValue
encodeBytea h = buildBS $ BSB.string7 "\\x" <> h

decodeBytea :: PGTextValue -> [Word8]
decodeBytea s
  | sm /= "\\x" = error $ "pgDecode bytea: " ++ sm
  | otherwise = pd $ BS.unpack d where
  (m, d) = BS.splitAt 2 s
  sm = BSC.unpack m
  pd [] = []
  pd (h:l:r) = (shiftL (unhex h) 4 .|. unhex l) : pd r
  pd [x] = error $ "pgDecode bytea: " ++ show x
  unhex = fromIntegral . digitToInt . w2c

instance PGParameter "bytea" BSL.ByteString where
  pgEncode _ = encodeBytea . BSB.lazyByteStringHex
  pgLiteral t = pgQuoteUnsafe . BSC.unpack . pgEncode t
instance PGColumn "bytea" BSL.ByteString where
  pgDecode _ = BSL.pack . decodeBytea
instance PGParameter "bytea" BS.ByteString where
  pgEncode _ = encodeBytea . BSB.byteStringHex
  pgLiteral t = pgQuoteUnsafe . BSC.unpack . pgEncode t
instance PGColumn "bytea" BS.ByteString where
  pgDecode _ = BS.pack . decodeBytea

instance PGParameter "date" Time.Day where
  pgEncode _ = BSC.pack . Time.showGregorian
  pgLiteral _ = pgQuoteUnsafe . Time.showGregorian
instance PGColumn "date" Time.Day where
  pgDecode _ = Time.readTime defaultTimeLocale "%F" . BSC.unpack

instance PGParameter "time without time zone" Time.TimeOfDay where
  pgEncode _ = BSC.pack . Time.formatTime defaultTimeLocale "%T%Q"
  pgLiteral _ = pgQuoteUnsafe . Time.formatTime defaultTimeLocale "%T%Q"
instance PGColumn "time without time zone" Time.TimeOfDay where
  pgDecode _ = Time.readTime defaultTimeLocale "%T%Q" . BSC.unpack

instance PGParameter "timestamp without time zone" Time.LocalTime where
  pgEncode _ = BSC.pack . Time.formatTime defaultTimeLocale "%F %T%Q"
  pgLiteral _ = pgQuoteUnsafe . Time.formatTime defaultTimeLocale "%F %T%Q"
instance PGColumn "timestamp without time zone" Time.LocalTime where
  pgDecode _ = Time.readTime defaultTimeLocale "%F %T%Q" . BSC.unpack

-- PostgreSQL uses "[+-]HH[:MM]" timezone offsets, while "%z" uses "+HHMM" by default.
-- readTime can successfully parse both formats, but PostgreSQL needs the colon.
fixTZ :: String -> String
fixTZ "" = ""
fixTZ ['+',h1,h2] | isDigit h1 && isDigit h2 = ['+',h1,h2,':','0','0']
fixTZ ['-',h1,h2] | isDigit h1 && isDigit h2 = ['-',h1,h2,':','0','0']
fixTZ ['+',h1,h2,m1,m2] | isDigit h1 && isDigit h2 && isDigit m1 && isDigit m2 = ['+',h1,h2,':',m1,m2]
fixTZ ['-',h1,h2,m1,m2] | isDigit h1 && isDigit h2 && isDigit m1 && isDigit m2 = ['-',h1,h2,':',m1,m2]
fixTZ (c:s) = c:fixTZ s

instance PGParameter "timestamp with time zone" Time.UTCTime where
  pgEncode _ = BSC.pack . fixTZ . Time.formatTime defaultTimeLocale "%F %T%Q%z"
  pgLiteral _ = pgQuote{-Unsafe-} . fixTZ . Time.formatTime defaultTimeLocale "%F %T%Q%z"
instance PGColumn "timestamp with time zone" Time.UTCTime where
  pgDecode _ = Time.readTime defaultTimeLocale "%F %T%Q%z" . fixTZ . BSC.unpack

instance PGParameter "interval" Time.DiffTime where
  pgEncode _ = BSC.pack . show
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
    | denominator r == 0 = BSC.pack "NaN" -- this can't happen
    | otherwise = BSC.pack $ take 30 (showRational (r / (10 ^^ e))) ++ 'e' : show e where
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
    s = BSC.unpack bs

-- This will produce infinite(-precision) strings
showRational :: Rational -> String
showRational r = show (ri :: Integer) ++ '.' : frac (abs rf) where
  (ri, rf) = properFraction r
  frac 0 = ""
  frac f = intToDigit i : frac f' where (i, f') = properFraction (10 * f)

#ifdef USE_SCIENTIFIC
instance PGLiteralType "numeric" Scientific
#endif

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
  pgEncode ta l = buildBS $ BSB.char7 '{' <> mconcat (intersperse (BSB.char7 $ pgArrayDelim ta) $ map el l) <> BSB.char7 '}' where
    el Nothing = BSB.string7 "null"
    el (Just e) = dQuote (pgArrayDelim ta : "{}") $ pgEncode (pgArrayElementType ta) e
instance (PGArrayType ta t, PGColumn t a) => PGColumn ta (PGArray a) where
  pgDecode ta = either (error . ("pgDecode array: " ++) . show) id . P.parse pa "array" where
    pa = do
      l <- P.between (P.char '{') (P.char '}') $
        P.sepBy nel (P.char (pgArrayDelim ta))
      _ <- P.eof
      return l
    nel = P.between P.spaces P.spaces $ Nothing <$ nul P.<|> Just <$> el
    nul = P.oneOf "Nn" >> P.oneOf "Uu" >> P.oneOf "Ll" >> P.oneOf "Ll"
    el = pgDecode (pgArrayElementType ta) . BSC.pack <$> parseDQuote (pgArrayDelim ta : "{}")

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


-- |Class indicating that the first PostgreSQL type is a range of the second.
-- This implies 'PGParameter' and 'PGColumn' instances that will work for any type.
class (KnownSymbol tr, KnownSymbol t) => PGRangeType tr t | tr -> t where
  pgRangeElementType :: PGTypeName tr -> PGTypeName t
  pgRangeElementType PGTypeProxy = PGTypeProxy

instance (PGRangeType tr t, PGParameter t a) => PGParameter tr (Range.Range a) where
  pgEncode _ Range.Empty = BSC.pack "empty"
  pgEncode tr (Range.Range (Range.Lower l) (Range.Upper u)) = buildBS $
    pc '[' '(' l
      <> pb (Range.bound l)
      <> BSB.char7 ','
      <> pb (Range.bound u)
      <> pc ']' ')' u
    where
    pb Nothing = mempty
    pb (Just b) = dQuote "(),[]" $ pgEncode (pgRangeElementType tr) b
    pc c o b = BSB.char7 $ if Range.boundClosed b then c else o
instance (PGRangeType tr t, PGColumn t a) => PGColumn tr (Range.Range a) where
  pgDecode tr = either (error . ("pgDecode range: " ++) . show) id . P.parse per "range" where
    per = Range.Empty <$ pe P.<|> pr
    pe = P.oneOf "Ee" >> P.oneOf "Mm" >> P.oneOf "Pp" >> P.oneOf "Tt" >> P.oneOf "Yy"
    pp = pgDecode (pgRangeElementType tr) . BSC.pack <$> parseDQuote "(),[]"
    pc c o = True <$ P.char c P.<|> False <$ P.char o
    pb = P.optionMaybe $ P.between P.spaces P.spaces $ pp
    mb = maybe Range.Unbounded . Range.Bounded
    pr = do
      lc <- pc '[' '('
      lb <- pb
      _ <- P.char ','
      ub <- pb 
      uc <- pc ']' ')'
      return $ Range.Range (Range.Lower (mb lc lb)) (Range.Upper (mb uc ub))

instance PGRangeType "int4range" "integer"
instance PGRangeType "numrange" "numeric"
instance PGRangeType "tsrange" "timestamp without time zone"
instance PGRangeType "tstzrange" "timestamp with time zone"
instance PGRangeType "daterange" "date"
instance PGRangeType "int8range" "bigint"

#ifdef USE_UUID
instance PGParameter "uuid" UUID.UUID where
  pgEncode _ = UUID.toASCIIBytes
  pgLiteral _ = pgQuoteUnsafe . UUID.toString
instance PGColumn "uuid" UUID.UUID where
  pgDecode _ u = fromMaybe (error $ "pgDecode uuid: " ++ BSC.unpack u) $ UUID.fromASCIIBytes u
#endif

#ifdef USE_BINARY
binDec :: KnownSymbol t => BinD.D a -> PGTypeName t -> PGBinaryValue -> a
binDec d t = either (\e -> error $ "pgDecodeBinary " ++ pgTypeName t ++ ": " ++ show e) id . d

instance PGBinaryType "oid"
instance PGBinaryParameter "oid" OID where
  pgEncodeBinary _ _ = BinE.int4 . Right
instance PGBinaryColumn "oid" OID where
  pgDecodeBinary _ = binDec BinD.int

instance PGBinaryType "smallint"
instance PGBinaryParameter "smallint" Int16 where
  pgEncodeBinary _ _ = BinE.int2 . Left
instance PGBinaryColumn "smallint" Int16 where
  pgDecodeBinary _ = binDec BinD.int

instance PGBinaryType "integer"
instance PGBinaryParameter "integer" Int32 where
  pgEncodeBinary _ _ = BinE.int4 . Left
instance PGBinaryColumn "integer" Int32 where
  pgDecodeBinary _ = binDec BinD.int

instance PGBinaryType "bigint"
instance PGBinaryParameter "bigint" Int64 where
  pgEncodeBinary _ _ = BinE.int8 . Left
instance PGBinaryColumn "bigint" Int64 where
  pgDecodeBinary _ = binDec BinD.int

instance PGBinaryType "real"
instance PGBinaryParameter "real" Float where
  pgEncodeBinary _ _ = BinE.float4
instance PGBinaryColumn "real" Float where
  pgDecodeBinary _ = binDec BinD.float4

instance PGBinaryType "double precision"
instance PGBinaryParameter "double precision" Double where
  pgEncodeBinary _ _ = BinE.float8
instance PGBinaryColumn "double precision" Double where
  pgDecodeBinary _ = binDec BinD.float8

instance PGBinaryType "numeric"
instance PGBinaryParameter "numeric" Scientific where
  pgEncodeBinary _ _ = BinE.numeric
instance PGBinaryColumn "numeric" Scientific where
  pgDecodeBinary _ = binDec BinD.numeric
instance PGBinaryParameter "numeric" Rational where
  pgEncodeBinary _ _ = BinE.numeric . realToFrac
instance PGBinaryColumn "numeric" Rational where
  pgDecodeBinary _ t = realToFrac . binDec BinD.numeric t

instance PGBinaryType "\"char\""
instance PGBinaryParameter "\"char\"" Char where
  pgEncodeBinary _ _ = BinE.char
instance PGBinaryColumn "\"char\"" Char where
  pgDecodeBinary _ = binDec BinD.char

instance PGBinaryType "text"
instance PGBinaryType "character varying"
instance PGBinaryType "bpchar"
instance PGBinaryType "name" -- not strictly textsend, but essentially the same
instance (PGStringType t, PGBinaryType t) => PGBinaryParameter t T.Text where
  pgEncodeBinary _ _ = BinE.text . Left
instance (PGStringType t, PGBinaryType t) => PGBinaryColumn t T.Text where
  pgDecodeBinary _ = binDec BinD.text
instance (PGStringType t, PGBinaryType t) => PGBinaryParameter t TL.Text where
  pgEncodeBinary _ _ = BinE.text . Right
instance (PGStringType t, PGBinaryType t) => PGBinaryColumn t TL.Text where
  pgDecodeBinary _ t = TL.fromStrict . binDec BinD.text t
instance (PGStringType t, PGBinaryType t) => PGBinaryParameter t BS.ByteString where
  pgEncodeBinary _ _ = BinE.text . Left . TE.decodeUtf8
instance (PGStringType t, PGBinaryType t) => PGBinaryColumn t BS.ByteString where
  pgDecodeBinary _ t = TE.encodeUtf8 . binDec BinD.text t
instance (PGStringType t, PGBinaryType t) => PGBinaryParameter t BSL.ByteString where
  pgEncodeBinary _ _ = BinE.text . Right . TLE.decodeUtf8
instance (PGStringType t, PGBinaryType t) => PGBinaryColumn t BSL.ByteString where
  pgDecodeBinary _ t = BSL.fromStrict . TE.encodeUtf8 . binDec BinD.text t
instance (PGStringType t, PGBinaryType t) => PGBinaryParameter t String where
  pgEncodeBinary _ _ = BinE.text . Left . T.pack
instance (PGStringType t, PGBinaryType t) => PGBinaryColumn t String where
  pgDecodeBinary _ t = T.unpack . binDec BinD.text t

instance PGBinaryType "bytea"
instance PGBinaryParameter "bytea" BS.ByteString where
  pgEncodeBinary _ _ = BinE.bytea . Left
instance PGBinaryColumn "bytea" BS.ByteString where
  pgDecodeBinary _ = binDec BinD.bytea
instance PGBinaryParameter "bytea" BSL.ByteString where
  pgEncodeBinary _ _ = BinE.bytea . Right
instance PGBinaryColumn "bytea" BSL.ByteString where
  pgDecodeBinary _ t = BSL.fromStrict . binDec BinD.bytea t

instance PGBinaryType "date"
instance PGBinaryParameter "date" Time.Day where
  pgEncodeBinary _ _ = BinE.date
instance PGBinaryColumn "date" Time.Day where
  pgDecodeBinary _ = binDec BinD.date
instance PGBinaryType "time without time zone"
instance PGBinaryParameter "time without time zone" Time.TimeOfDay where
  pgEncodeBinary e _ = BinE.time (pgIntegerDatetimes e)
instance PGBinaryColumn "time without time zone" Time.TimeOfDay where
  pgDecodeBinary e = binDec $ BinD.time (pgIntegerDatetimes e)
instance PGBinaryType "timestamp without time zone"
instance PGBinaryParameter "timestamp without time zone" Time.LocalTime where
  pgEncodeBinary e _ = BinE.timestamp (pgIntegerDatetimes e)
instance PGBinaryColumn "timestamp without time zone" Time.LocalTime where
  pgDecodeBinary e = binDec $ BinD.timestamp (pgIntegerDatetimes e)
instance PGBinaryType "timestamp with time zone"
instance PGBinaryParameter "timestamp with time zone" Time.UTCTime where
  pgEncodeBinary e _ = BinE.timestamptz (pgIntegerDatetimes e)
instance PGBinaryColumn "timestamp with time zone" Time.UTCTime where
  pgDecodeBinary e = binDec $ BinD.timestamptz (pgIntegerDatetimes e)
instance PGBinaryType "interval"
instance PGBinaryParameter "interval" Time.DiffTime where
  pgEncodeBinary e _ = BinE.interval (pgIntegerDatetimes e)
instance PGBinaryColumn "interval" Time.DiffTime where
  pgDecodeBinary e = binDec $ BinD.interval (pgIntegerDatetimes e)

instance PGBinaryType "boolean"
instance PGBinaryParameter "boolean" Bool where
  pgEncodeBinary _ _ = BinE.bool
instance PGBinaryColumn "boolean" Bool where
  pgDecodeBinary _ = binDec BinD.bool

instance PGBinaryType "uuid"
instance PGBinaryParameter "uuid" UUID.UUID where
  pgEncodeBinary _ _ = BinE.uuid
instance PGBinaryColumn "uuid" UUID.UUID where
  pgDecodeBinary _ = binDec BinD.uuid

-- TODO: arrays (a bit complicated, need OID?, but theoretically possible)
#endif

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
-}
