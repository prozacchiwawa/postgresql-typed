{-# LANGUAGE CPP, FlexibleInstances, ScopedTypeVariables, MultiParamTypeClasses, FunctionalDependencies, FlexibleContexts, DataKinds, KindSignatures, TypeFamilies, UndecidableInstances #-}
#if __GLASGOW_HASKELL__ < 710
{-# LANGUAGE OverlappingInstances #-}
#endif
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
  , PGTypeName(..)
  , PGTypeEnv(..)
  , unknownPGTypeEnv
  , PGRecord(..)

  -- * Marshalling classes
  , PGType(..)
  , PGParameter(..)
  , PGColumn(..)

  -- * Marshalling interface
  , pgEncodeParameter
  , pgEscapeParameter
  , pgDecodeColumn
  , pgDecodeColumnNotNull

  -- * Conversion utilities
  , pgQuote
  , pgDQuote
  , parsePGDQuote
  , buildPGValue
  ) where

#if !MIN_VERSION_base(4,8,0)
import Control.Applicative ((<$>), (<$), (<*), (*>))
#endif
#ifdef USE_AESON
import qualified Data.Aeson as JSON
#endif
import qualified Data.Attoparsec.ByteString as P (anyWord8)
import qualified Data.Attoparsec.ByteString.Char8 as P
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
import Data.Monoid ((<>))
#if !MIN_VERSION_base(4,8,0)
import Data.Monoid (mconcat, mempty)
#endif
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
#if MIN_VERSION_time(1,5,0)
import Data.Time (defaultTimeLocale)
#else
import System.Locale (defaultTimeLocale)
#endif
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

type PGTextValue = BS.ByteString
type PGBinaryValue = BS.ByteString
-- |A value passed to or from PostgreSQL in raw format.
data PGValue
  = PGNullValue
  | PGTextValue { pgTextValue :: PGTextValue } -- ^ The standard text encoding format (also used for unknown formats)
  | PGBinaryValue { pgBinaryValue :: PGBinaryValue } -- ^ Special binary-encoded data.  Not supported in all cases.
  deriving (Show, Eq)
-- |A list of (nullable) data values, e.g. a single row or query parameters.
type PGValues = [PGValue]

-- |Parameters that affect how marshalling happens.
-- Currenly we force all other relevant parameters at connect time.
-- Nothing values represent unknown.
data PGTypeEnv = PGTypeEnv
  { pgIntegerDatetimes :: Maybe Bool -- ^ If @integer_datetimes@ is @on@; only relevant for binary encoding.
  } deriving (Show)

unknownPGTypeEnv :: PGTypeEnv
unknownPGTypeEnv = PGTypeEnv
  { pgIntegerDatetimes = Nothing
  }

-- |A proxy type for PostgreSQL types.  The type argument should be an (internal) name of a database type (see @\\dT+@).
data PGTypeName (t :: Symbol) = PGTypeProxy

-- |A valid PostgreSQL type.
-- This is just an indicator class: no implementation is needed.
-- Unfortunately this will generate orphan instances wherever used.
class KnownSymbol t => PGType t where
  pgTypeName :: PGTypeName t -> String
  pgTypeName = symbolVal
  -- |Does this type support binary decoding?
  -- If so, 'pgDecodeBinary' must be implemented for every 'PGColumn' instance of this type.
  pgBinaryColumn :: PGTypeEnv -> PGTypeName t -> Bool
  pgBinaryColumn _ _ = False

-- |A @PGParameter t a@ instance describes how to encode a PostgreSQL type @t@ from @a@.
class PGType t => PGParameter t a where
  -- |Encode a value to a PostgreSQL text representation.
  pgEncode :: PGTypeName t -> a -> PGTextValue
  -- |Encode a value to a (quoted) literal value for use in SQL statements.
  -- Defaults to a quoted version of 'pgEncode'
  pgLiteral :: PGTypeName t -> a -> BS.ByteString
  pgLiteral t = pgQuote . pgEncode t
  -- |Encode a value to a PostgreSQL representation.
  -- Defaults to the text representation by pgEncode
  pgEncodeValue :: PGTypeEnv -> PGTypeName t -> a -> PGValue
  pgEncodeValue _ t = PGTextValue . pgEncode t

-- |A @PGColumn t a@ instance describes how te decode a PostgreSQL type @t@ to @a@.
class PGType t => PGColumn t a where
  -- |Decode the PostgreSQL text representation into a value.
  pgDecode :: PGTypeName t -> PGTextValue -> a
  -- |Decode the PostgreSQL binary representation into a value.
  -- Only needs to be implemented if 'pgBinaryColumn' is true.
  pgDecodeBinary :: PGTypeEnv -> PGTypeName t -> PGBinaryValue -> a
  pgDecodeBinary _ t _ = error $ "pgDecodeBinary " ++ pgTypeName t ++ ": not supported"
  pgDecodeValue :: PGTypeEnv -> PGTypeName t -> PGValue -> a
  pgDecodeValue _ t (PGTextValue v) = pgDecode t v
  pgDecodeValue e t (PGBinaryValue v) = pgDecodeBinary e t v
  pgDecodeValue _ t PGNullValue = error $ "NULL in " ++ pgTypeName t ++ " column (use Maybe or COALESCE)"

instance PGParameter t a => PGParameter t (Maybe a) where
  pgEncode t = maybe (error $ "pgEncode " ++ pgTypeName t ++ ": Nothing") (pgEncode t)
  pgLiteral = maybe (BSC.pack "NULL") . pgLiteral
  pgEncodeValue e = maybe PGNullValue . pgEncodeValue e

instance PGColumn t a => PGColumn t (Maybe a) where
  pgDecode t = Just . pgDecode t
  pgDecodeBinary e t = Just . pgDecodeBinary e t
  pgDecodeValue _ _ PGNullValue = Nothing
  pgDecodeValue e t v = Just $ pgDecodeValue e t v


-- |Final parameter encoding function used when a (nullable) parameter is passed to a prepared query.
pgEncodeParameter :: PGParameter t a => PGTypeEnv -> PGTypeName t -> a -> PGValue
pgEncodeParameter = pgEncodeValue

-- |Final parameter escaping function used when a (nullable) parameter is passed to be substituted into a simple query.
pgEscapeParameter :: PGParameter t a => PGTypeEnv -> PGTypeName t -> a -> BS.ByteString
pgEscapeParameter _ = pgLiteral

-- |Final column decoding function used for a nullable result value.
pgDecodeColumn :: PGColumn t (Maybe a) => PGTypeEnv -> PGTypeName t -> PGValue -> Maybe a
pgDecodeColumn = pgDecodeValue

-- |Final column decoding function used for a non-nullable result value.
pgDecodeColumnNotNull :: PGColumn t a => PGTypeEnv -> PGTypeName t -> PGValue -> a
pgDecodeColumnNotNull = pgDecodeValue


pgQuoteUnsafe :: BS.ByteString -> BS.ByteString
pgQuoteUnsafe = (`BSC.snoc` '\'') . BSC.cons '\''

-- |Produce a SQL string literal by wrapping (and escaping) a string with single quotes.
pgQuote :: BS.ByteString -> BS.ByteString
pgQuote = pgQuoteUnsafe . BSC.intercalate (BSC.pack "''") . BSC.split '\''

buildPGValue :: BSB.Builder -> BS.ByteString
buildPGValue = BSL.toStrict . BSB.toLazyByteString

-- |Double-quote a value if it's \"\", \"null\", or contains any whitespace, \'\"\', \'\\\', or the characters given in the first argument.
-- Checking all these things may not be worth it.  We could just double-quote everything.
pgDQuote :: [Char] -> BS.ByteString -> BSB.Builder
pgDQuote unsafe s
  | BS.null s || BSC.any (\c -> isSpace c || c == '"' || c == '\\' || c `elem` unsafe) s || BSC.map toLower s == BSC.pack "null" =
    dq <> BSBP.primMapByteStringBounded ec s <> dq
  | otherwise = BSB.byteString s where
  dq = BSB.char7 '"'
  ec = BSBP.condB (\c -> c == c2w '"' || c == c2w '\\') bs (BSBP.liftFixedToBounded BSBP.word8)
  bs = BSBP.liftFixedToBounded $ ((,) '\\') BSBP.>$< (BSBP.char7 BSBP.>*< BSBP.word8)

-- |Parse double-quoted values ala 'pgDQuote'.
parsePGDQuote :: Bool -> [Char] -> (BS.ByteString -> Bool) -> P.Parser (Maybe BS.ByteString)
parsePGDQuote blank unsafe isnul = (Just <$> q) <> (mnul <$> uq) where
  q = P.char '"' *> (BS.concat <$> qs)
  qs = do
    p <- P.takeTill (\c -> c == '"' || c == '\\')
    e <- P.anyChar
    if e == '"'
      then return [p]
      else do
        c <- P.anyWord8
        (p :) . (BS.singleton c :) <$> qs
  uq = (if blank then P.takeWhile else P.takeWhile1) (`notElem` ('"':'\\':unsafe))
  mnul s
    | isnul s = Nothing
    | otherwise = Just s

#ifdef USE_BINARY
binDec :: PGType t => BinD.D a -> PGTypeName t -> PGBinaryValue -> a
binDec d t = either (\e -> error $ "pgDecodeBinary " ++ pgTypeName t ++ ": " ++ show e) id . d

#define BIN_COL pgBinaryColumn _ _ = True
#define BIN_ENC(F) pgEncodeValue _ _ = PGBinaryValue . F
#define BIN_DEC(F) pgDecodeBinary _ = F
#else
#define BIN_COL
#define BIN_ENC(F)
#define BIN_DEC(F)
#endif

instance PGType "void"
instance PGColumn "void" () where
  pgDecode _ _ = ()
  pgDecodeBinary _ _ _ = ()
  pgDecodeValue _ _ _ = ()

instance PGType "boolean" where BIN_COL
instance PGParameter "boolean" Bool where
  pgEncode _ False = BSC.singleton 'f'
  pgEncode _ True = BSC.singleton 't'
  pgLiteral _ False = BSC.pack "false"
  pgLiteral _ True = BSC.pack "true"
  BIN_ENC(BinE.bool)
instance PGColumn "boolean" Bool where
  pgDecode _ s = case BSC.head s of
    'f' -> False
    't' -> True
    c -> error $ "pgDecode boolean: " ++ [c]
  BIN_DEC(binDec BinD.bool)

type OID = Word32
instance PGType "oid" where BIN_COL
instance PGParameter "oid" OID where
  pgEncode _ = BSC.pack . show
  pgLiteral = pgEncode
  BIN_ENC(BinE.int4 . Right)
instance PGColumn "oid" OID where
  pgDecode _ = read . BSC.unpack
  BIN_DEC(binDec BinD.int)

instance PGType "smallint" where BIN_COL
instance PGParameter "smallint" Int16 where
  pgEncode _ = BSC.pack . show
  pgLiteral = pgEncode
  BIN_ENC(BinE.int2. Left)
instance PGColumn "smallint" Int16 where
  pgDecode _ = read . BSC.unpack
  BIN_DEC(binDec BinD.int)

instance PGType "integer" where BIN_COL
instance PGParameter "integer" Int32 where
  pgEncode _ = BSC.pack . show
  pgLiteral = pgEncode
  BIN_ENC(BinE.int4 . Left)
instance PGColumn "integer" Int32 where
  pgDecode _ = read . BSC.unpack
  BIN_DEC(binDec BinD.int)

instance PGType "bigint" where BIN_COL
instance PGParameter "bigint" Int64 where
  pgEncode _ = BSC.pack . show
  pgLiteral = pgEncode
  BIN_ENC(BinE.int8 . Left)
instance PGColumn "bigint" Int64 where
  pgDecode _ = read . BSC.unpack
  BIN_DEC(binDec BinD.int)

instance PGType "real" where BIN_COL
instance PGParameter "real" Float where
  pgEncode _ = BSC.pack . show
  pgLiteral = pgEncode
  BIN_ENC(BinE.float4)
instance PGColumn "real" Float where
  pgDecode _ = read . BSC.unpack
  BIN_DEC(binDec BinD.float4)

instance PGType "double precision" where BIN_COL
instance PGParameter "double precision" Double where
  pgEncode _ = BSC.pack . show
  pgLiteral = pgEncode
  BIN_ENC(BinE.float8)
instance PGColumn "double precision" Double where
  pgDecode _ = read . BSC.unpack
  BIN_DEC(binDec BinD.float8)

instance PGType "\"char\"" where BIN_COL
instance PGParameter "\"char\"" Char where
  pgEncode _ = BSC.singleton
  BIN_ENC(BinE.char)
instance PGColumn "\"char\"" Char where
  pgDecode _ = BSC.head
  BIN_DEC(binDec BinD.char)


class PGType t => PGStringType t

instance PGStringType t => PGParameter t String where
  pgEncode _ = BSU.fromString
  BIN_ENC(BinE.text . Left . T.pack)
instance PGStringType t => PGColumn t String where
  pgDecode _ = BSU.toString
  BIN_DEC((T.unpack .) . binDec BinD.text)

instance {-# OVERLAPPABLE #-} PGStringType t => PGParameter t BS.ByteString where
  pgEncode _ = id
  BIN_ENC(BinE.text . Left . TE.decodeUtf8)
instance {-# OVERLAPPABLE #-} PGStringType t => PGColumn t BS.ByteString where
  pgDecode _ = id
  BIN_DEC((TE.encodeUtf8 .) . binDec BinD.text)

instance {-# OVERLAPPABLE #-} PGStringType t => PGParameter t BSL.ByteString where
  pgEncode _ = BSL.toStrict
  BIN_ENC(BinE.text . Right . TLE.decodeUtf8)
instance {-# OVERLAPPABLE #-} PGStringType t => PGColumn t BSL.ByteString where
  pgDecode _ = BSL.fromStrict
  BIN_DEC((BSL.fromStrict .) . (TE.encodeUtf8 .) . binDec BinD.text)

#ifdef USE_TEXT
instance PGStringType t => PGParameter t T.Text where
  pgEncode _ = TE.encodeUtf8
  BIN_ENC(BinE.text . Left)
instance PGStringType t => PGColumn t T.Text where
  pgDecode _ = TE.decodeUtf8
  BIN_DEC(binDec BinD.text)

instance PGStringType t => PGParameter t TL.Text where
  pgEncode _ = BSL.toStrict . TLE.encodeUtf8
  BIN_ENC(BinE.text . Right)
instance PGStringType t => PGColumn t TL.Text where
  pgDecode _ = TL.fromStrict . TE.decodeUtf8
  BIN_DEC((TL.fromStrict .) . binDec BinD.text)
#endif

instance PGType "text" where BIN_COL
instance PGType "character varying" where BIN_COL
instance PGType "name" where BIN_COL
instance PGType "bpchar" where BIN_COL
instance PGStringType "text"
instance PGStringType "character varying"
instance PGStringType "name" -- limit 63 characters; not strictly textsend but essentially the same
instance PGStringType "bpchar" -- blank padded


encodeBytea :: BSB.Builder -> PGTextValue
encodeBytea h = buildPGValue $ BSB.string7 "\\x" <> h

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

instance PGType "bytea" where BIN_COL
instance {-# OVERLAPPING #-} PGParameter "bytea" BSL.ByteString where
  pgEncode _ = encodeBytea . BSB.lazyByteStringHex
  pgLiteral t = pgQuoteUnsafe . pgEncode t
  BIN_ENC(BinE.bytea . Right)
instance {-# OVERLAPPING #-} PGColumn "bytea" BSL.ByteString where
  pgDecode _ = BSL.pack . decodeBytea
  BIN_DEC((BSL.fromStrict .) . binDec BinD.bytea)
instance {-# OVERLAPPING #-} PGParameter "bytea" BS.ByteString where
  pgEncode _ = encodeBytea . BSB.byteStringHex
  pgLiteral t = pgQuoteUnsafe . pgEncode t
  BIN_ENC(BinE.bytea . Left)
instance {-# OVERLAPPING #-} PGColumn "bytea" BS.ByteString where
  pgDecode _ = BS.pack . decodeBytea
  BIN_DEC(binDec BinD.bytea)

readTime :: Time.ParseTime t => String -> String -> t
readTime =
#if MIN_VERSION_time(1,5,0)
  Time.parseTimeOrError False
#else
  Time.readTime
#endif
    defaultTimeLocale 

instance PGType "date" where BIN_COL
instance PGParameter "date" Time.Day where
  pgEncode _ = BSC.pack . Time.showGregorian
  pgLiteral t = pgQuoteUnsafe . pgEncode t
  BIN_ENC(BinE.date)
instance PGColumn "date" Time.Day where
  pgDecode _ = readTime "%F" . BSC.unpack
  BIN_DEC(binDec BinD.date)

binColDatetime :: PGTypeEnv -> PGTypeName t -> Bool
#ifdef USE_BINARY
binColDatetime PGTypeEnv{ pgIntegerDatetimes = Just _ } _ = True
#endif
binColDatetime _ _ = False

#ifdef USE_BINARY
binEncDatetime :: PGParameter t a => (Bool -> a -> PGBinaryValue) -> PGTypeEnv -> PGTypeName t -> a -> PGValue
binEncDatetime f e t = maybe (PGTextValue . pgEncode t) ((PGBinaryValue .) . f) (pgIntegerDatetimes e)

binDecDatetime :: PGColumn t a => (Bool -> BinD.D a) -> PGTypeEnv -> PGTypeName t -> PGBinaryValue -> a
binDecDatetime f e = binDec $ f $ fromMaybe (error "pgDecodeBinary: unknown integer_datetimes value") $ pgIntegerDatetimes e
#endif

instance PGType "time without time zone" where
  pgBinaryColumn = binColDatetime
instance PGParameter "time without time zone" Time.TimeOfDay where
  pgEncode _ = BSC.pack . Time.formatTime defaultTimeLocale "%T%Q"
  pgLiteral t = pgQuoteUnsafe . pgEncode t
#ifdef USE_BINARY
  pgEncodeValue = binEncDatetime BinE.time
#endif
instance PGColumn "time without time zone" Time.TimeOfDay where
  pgDecode _ = readTime "%T%Q" . BSC.unpack
#ifdef USE_BINARY
  pgDecodeBinary = binDecDatetime BinD.time
#endif

instance PGType "timestamp without time zone" where
  pgBinaryColumn = binColDatetime
instance PGParameter "timestamp without time zone" Time.LocalTime where
  pgEncode _ = BSC.pack . Time.formatTime defaultTimeLocale "%F %T%Q"
  pgLiteral t = pgQuoteUnsafe . pgEncode t
#ifdef USE_BINARY
  pgEncodeValue = binEncDatetime BinE.timestamp
#endif
instance PGColumn "timestamp without time zone" Time.LocalTime where
  pgDecode _ = readTime "%F %T%Q" . BSC.unpack
#ifdef USE_BINARY
  pgDecodeBinary = binDecDatetime BinD.timestamp
#endif

-- PostgreSQL uses "[+-]HH[:MM]" timezone offsets, while "%z" uses "+HHMM" by default.
-- readTime can successfully parse both formats, but PostgreSQL needs the colon.
fixTZ :: String -> String
fixTZ "" = ""
fixTZ ['+',h1,h2] | isDigit h1 && isDigit h2 = ['+',h1,h2,':','0','0']
fixTZ ['-',h1,h2] | isDigit h1 && isDigit h2 = ['-',h1,h2,':','0','0']
fixTZ ['+',h1,h2,m1,m2] | isDigit h1 && isDigit h2 && isDigit m1 && isDigit m2 = ['+',h1,h2,':',m1,m2]
fixTZ ['-',h1,h2,m1,m2] | isDigit h1 && isDigit h2 && isDigit m1 && isDigit m2 = ['-',h1,h2,':',m1,m2]
fixTZ (c:s) = c:fixTZ s

instance PGType "timestamp with time zone" where
  pgBinaryColumn = binColDatetime
instance PGParameter "timestamp with time zone" Time.UTCTime where
  pgEncode _ = BSC.pack . fixTZ . Time.formatTime defaultTimeLocale "%F %T%Q%z"
  -- pgLiteral t = pgQuoteUnsafe . pgEncode t
#ifdef USE_BINARY
  pgEncodeValue = binEncDatetime BinE.timestamptz
#endif
instance PGColumn "timestamp with time zone" Time.UTCTime where
  pgDecode _ = readTime "%F %T%Q%z" . fixTZ . BSC.unpack
#ifdef USE_BINARY
  pgDecodeBinary = binDecDatetime BinD.timestamptz
#endif

instance PGType "interval" where
  pgBinaryColumn = binColDatetime
instance PGParameter "interval" Time.DiffTime where
  pgEncode _ = BSC.pack . show
  pgLiteral t = pgQuoteUnsafe . pgEncode t
#ifdef USE_BINARY
  pgEncodeValue = binEncDatetime BinE.interval
#endif
-- |Representation of DiffTime as interval.
-- PostgreSQL stores months and days separately in intervals, but DiffTime does not.
-- We collapse all interval fields into seconds
instance PGColumn "interval" Time.DiffTime where
  pgDecode _ a = either (error . ("pgDecode interval (" ++) . (++ ("): " ++ BSC.unpack a))) realToFrac $ P.parseOnly ps a where
    ps = do
      _ <- P.char 'P'
      d <- units [('Y', 12*month), ('M', month), ('W', 7*day), ('D', day)]
      ((d +) <$> pt) <> (d <$ P.endOfInput)
    pt = do
      _ <- P.char 'T'
      t <- units [('H', 3600), ('M', 60), ('S', 1)]
      P.endOfInput
      return t
    units l = fmap sum $ P.many' $ do
      x <- P.signed P.scientific
      u <- P.choice $ map (\(c, u) -> u <$ P.char c) l
      return $ x * u
    day = 86400
    month = 2629746
#ifdef USE_BINARY
  pgDecodeBinary = binDecDatetime BinD.interval
#endif

instance PGType "numeric" where BIN_COL
instance PGParameter "numeric" Rational where
  pgEncode _ r
    | denominator r == 0 = BSC.pack "NaN" -- this can't happen
    | otherwise = BSC.pack $ take 30 (showRational (r / (10 ^^ e))) ++ 'e' : show e where
    e = floor $ logBase (10 :: Double) $ fromRational $ abs r :: Int -- not great, and arbitrarily truncate somewhere
  pgLiteral _ r
    | denominator r == 0 = BSC.pack "'NaN'" -- this can't happen
    | otherwise = BSC.pack $ '(' : show (numerator r) ++ '/' : show (denominator r) ++ "::numeric)"
  BIN_ENC(BinE.numeric . realToFrac)
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
  BIN_DEC((realToFrac .) . binDec BinD.numeric)

-- This will produce infinite(-precision) strings
showRational :: Rational -> String
showRational r = show (ri :: Integer) ++ '.' : frac (abs rf) where
  (ri, rf) = properFraction r
  frac 0 = ""
  frac f = intToDigit i : frac f' where (i, f') = properFraction (10 * f)

#ifdef USE_SCIENTIFIC
instance PGParameter "numeric" Scientific where
  pgEncode _ = BSC.pack . show
  pgLiteral = pgEncode
  BIN_ENC(BinE.numeric)
instance PGColumn "numeric" Scientific where
  pgDecode _ = read . BSC.unpack
  BIN_DEC(binDec BinD.numeric)
#endif

#ifdef USE_UUID
instance PGType "uuid" where BIN_COL
instance PGParameter "uuid" UUID.UUID where
  pgEncode _ = UUID.toASCIIBytes
  pgLiteral t = pgQuoteUnsafe . pgEncode t
  BIN_ENC(BinE.uuid)
instance PGColumn "uuid" UUID.UUID where
  pgDecode _ u = fromMaybe (error $ "pgDecode uuid: " ++ BSC.unpack u) $ UUID.fromASCIIBytes u
  BIN_DEC(binDec BinD.uuid)
#endif

-- |Generic class of composite (row or record) types.
newtype PGRecord = PGRecord [Maybe PGTextValue]
class PGType t => PGRecordType t
instance PGRecordType t => PGParameter t PGRecord where
  pgEncode _ (PGRecord l) =
    buildPGValue $ BSB.char7 '(' <> mconcat (intersperse (BSB.char7 ',') $ map (maybe mempty (pgDQuote "(),")) l) <> BSB.char7 ')'
  pgLiteral _ (PGRecord l) =
    BSC.pack "ROW(" <> BS.intercalate (BSC.singleton ',') (map (maybe (BSC.pack "NULL") pgQuote) l) `BSC.snoc` ')'
instance PGRecordType t => PGColumn t PGRecord where
  pgDecode _ a = either (error . ("pgDecode record (" ++) . (++ ("): " ++ BSC.unpack a))) PGRecord $ P.parseOnly pa a where
    pa = P.char '(' *> P.sepBy el (P.char ',') <* P.char ')' <* P.endOfInput
    el = parsePGDQuote True "()," BS.null

instance PGType "record"
-- |The generic anonymous record type, as created by @ROW@.
-- In this case we can not know the types, and in fact, PostgreSQL does not accept values of this type regardless (except as literals).
instance PGRecordType "record"

#ifdef USE_AESON
instance PGType "json"
instance PGParameter "json" JSON.Value where
  pgEncode _ = BSL.toStrict . JSON.encode
instance PGColumn "json" JSON.Value where
  pgDecode _ j = either (error . ("pgDecode json (" ++) . (++ ("): " ++ BSC.unpack j))) id $ P.parseOnly JSON.json j

instance PGType "jsonb"
instance PGParameter "jsonb" JSON.Value where
  pgEncode _ = BSL.toStrict . JSON.encode
instance PGColumn "jsonb" JSON.Value where
  pgDecode _ j = either (error . ("pgDecode jsonb (" ++) . (++ ("): " ++ BSC.unpack j))) id $ P.parseOnly JSON.json j
#endif

{-
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
