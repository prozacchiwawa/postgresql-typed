{-# LANGUAGE ExistentialQuantification, TypeSynonymInstances, FlexibleInstances, OverlappingInstances, ScopedTypeVariables, MultiParamTypeClasses, FunctionalDependencies #-}
-- Copyright 2010, 2011, 2013 Chris Forno
-- Copyright 2014 Dylan Simon

module Database.TemplatePG.Types 
  ( pgQuote
  , PGType(..)
  , OID
  , PossiblyMaybe(..)
  , PGTypeHandler(..)
  , pgTypeDecoder
  , pgTypeEncoder
  , pgTypeEscaper
  , PGTypeMap
  , defaultTypeMap
  , pgArrayType
  ) where

import Control.Applicative ((<$>), (<$))
import Control.Monad (mzero)
import Data.Bits (shiftL, shiftR, (.|.), (.&.))
import Data.ByteString.Internal (w2c)
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.Char8 as LC
import qualified Data.ByteString.Lazy.UTF8 as U
import Data.Char (isDigit, digitToInt, intToDigit)
import Data.Int
import Data.List (intercalate)
import qualified Data.Map as Map
import Data.Maybe (fromMaybe)
import Data.Ratio ((%), numerator, denominator)
import qualified Data.Time as Time
import Data.Word (Word32)
import qualified Language.Haskell.TH as TH
import Numeric (readFloat)
import System.Locale (defaultTimeLocale)
import qualified Text.Parsec as P
import Text.Parsec.Token (naturalOrFloat, makeTokenParser, GenLanguageDef(..))

pgQuoteUnsafe :: String -> String
pgQuoteUnsafe s = '\'' : s ++ "'"

-- |Produce a SQL string literal by wrapping (and escaping) a string with single quotes.
pgQuote :: String -> String
pgQuote = ('\'':) . es where
  es "" = "'"
  es (c@'\'':r) = c:c:es r
  es (c:r) = c:es r

-- |Any type which can be marshalled to and from PostgreSQL.
-- Minimal definition: 'pgDecodeBS' (or 'pgDecode') and 'pgEncode' (or 'pgEncodeBS')
-- The default implementations do UTF-8 conversion.
class PGType a where
  -- |Decode a postgres raw text representation into a value.
  pgDecodeBS :: L.ByteString -> a
  pgDecodeBS = pgDecode . U.toString
  -- |Decode a postgres unicode string representation into a value.
  pgDecode :: String -> a
  pgDecode = pgDecodeBS . U.fromString
  -- |Encode a value to a postgres raw text representation.
  pgEncodeBS :: a -> L.ByteString
  pgEncodeBS = U.fromString . pgEncode
  -- |Encode a value to a postgres unicode representation.
  pgEncode :: a -> String
  pgEncode = U.toString . pgEncodeBS
  -- |Encode a value to a quoted literal value for use in statements.
  pgLiteral :: a -> String
  pgLiteral = pgQuote . pgEncode

instance PGType Bool where
  pgDecode "f" = False
  pgDecode "t" = True
  pgDecode s = error $ "pgDecode bool: " ++ s
  pgEncode False = "f"
  pgEncode True = "t"
  pgLiteral False = "false"
  pgLiteral True = "true"

type OID = Word32
instance PGType OID where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = read
  pgEncode = show
  pgLiteral = show

instance PGType Int where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = read
  pgEncode = show
  pgLiteral = show

instance PGType Int16 where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = read
  pgEncode = show
  pgLiteral = show

instance PGType Int32 where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = read
  pgEncode = show
  pgLiteral = show

instance PGType Int64 where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = read
  pgEncode = show
  pgLiteral = show

instance PGType Char where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode [c] = c
  pgDecode s = error $ "pgDecode char: " ++ s
  pgEncode c
    | fromEnum c < 256 = [c]
    | otherwise = error "pgEncode: Char out of range"

instance PGType Float where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = read
  pgEncode = show
  pgLiteral = show

instance PGType Double where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = read
  pgEncode = show
  pgLiteral = show

instance PGType String where
  pgDecode = id
  pgEncode = id

type Bytea = L.ByteString
instance PGType Bytea where
  pgDecode = pgDecodeBS . LC.pack
  pgEncodeBS = LC.pack . pgEncode
  pgDecodeBS s
    | sm /= "\\x" = error $ "pgDecode bytea: " ++ sm
    | otherwise = L.pack $ pd $ L.unpack d where
    (m, d) = L.splitAt 2 s
    sm = LC.unpack m
    pd [] = []
    pd (h:l:r) = (shiftL (unhex h) 4 .|. unhex l) : pd r
    pd [x] = error $ "pgDecode bytea: " ++ show x
    unhex = fromIntegral . digitToInt . w2c
  pgEncode = (++) "'\\x" . ed . L.unpack where
    ed [] = "\'"
    ed (x:d) = hex (shiftR x 4) : hex (x .&. 0xF) : ed d
    hex = intToDigit . fromIntegral
  pgLiteral = pgQuoteUnsafe . pgEncode

instance PGType Time.Day where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = Time.readTime defaultTimeLocale "%F"
  pgEncode = Time.showGregorian
  pgLiteral = pgQuoteUnsafe . pgEncode

instance PGType Time.TimeOfDay where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = Time.readTime defaultTimeLocale "%T%Q"
  pgEncode = Time.formatTime defaultTimeLocale "%T%Q"
  pgLiteral = pgQuoteUnsafe . pgEncode

instance PGType Time.LocalTime where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = Time.readTime defaultTimeLocale "%F %T%Q"
  pgEncode = Time.formatTime defaultTimeLocale "%F %T%Q"
  pgLiteral = pgQuoteUnsafe . pgEncode

instance PGType Time.ZonedTime where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = Time.readTime defaultTimeLocale "%F %T%Q%z" . fixTZ
  pgEncode = fixTZ . Time.formatTime defaultTimeLocale "%F %T%Q%z"
  pgLiteral = pgQuoteUnsafe . pgEncode

-- PostgreSQL uses "[+-]HH[:MM]" timezone offsets, while "%z" uses "+HHMM" by default.
-- readTime can successfully parse both formats, but PostgreSQL needs the colon.
fixTZ :: String -> String
fixTZ "" = ""
fixTZ ['+',h1,h2] | isDigit h1 && isDigit h2 = ['+',h1,h2,':','0','0']
fixTZ ['-',h1,h2] | isDigit h1 && isDigit h2 = ['-',h1,h2,':','0','0']
fixTZ ['+',h1,h2,m1,m2] | isDigit h1 && isDigit h2 && isDigit m1 && isDigit m2 = ['+',h1,h2,':',m1,m2]
fixTZ ['-',h1,h2,m1,m2] | isDigit h1 && isDigit h2 && isDigit m1 && isDigit m2 = ['-',h1,h2,':',m1,m2]
fixTZ (c:s) = c:fixTZ s

-- |Representation of DiffTime as interval.
-- PostgreSQL stores months and days separately in intervals, but DiffTime does not.
-- We collapse all interval fields into seconds
instance PGType Time.DiffTime where
  pgDecode = pgDecodeBS . LC.pack
  pgEncodeBS = LC.pack . pgEncode
  pgDecodeBS = either (error . ("pgDecode interval: " ++) . show) id . P.parse ps "interval" where
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
  pgEncode = show
  pgLiteral = pgQuoteUnsafe . pgEncode -- could be more efficient

-- |High-precision representation of Rational as numeric.
-- Unfortunately, numeric has an NaN, while Rational does not.
-- NaN numeric values will thus produce exceptions.
instance PGType Rational where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode "NaN" = 0 % 0 -- this won't work
  pgDecode s = ur $ readFloat s where
    ur [(x,"")] = x
    ur _ = error $ "pgDecode numeric: " ++ s
  pgEncode r
    | denominator r == 0 = "NaN" -- this can't happen
    | otherwise = take 30 (showRational (r / (10 ^^ e))) ++ 'e' : show e where
    e = floor $ logBase 10 $ fromRational $ abs r -- not great, and arbitrarily truncate somewhere
  pgLiteral r
    | denominator r == 0 = "'NaN'" -- this can't happen
    | otherwise = '(' : show (numerator r) ++ '/' : show (denominator r) ++ "::numeric)"

-- This will produce infinite(-precision) strings
showRational :: Rational -> String
showRational r = show (ri :: Integer) ++ '.' : frac (abs rf) where
  (ri, rf) = properFraction r
  frac 0 = ""
  frac f = intToDigit i : frac f' where (i, f') = properFraction (10 * f)

instance PGType a => PGType [Maybe a] where
  pgDecodeBS = either (error . ("pgDecode array: " ++) . show) id . P.parse pa "array" where
    pa = do
      l <- P.between (P.char '{') (P.char '}') $
        P.sepBy nel (P.char ',')
      _ <- P.eof
      return l
    nel = Nothing <$ nul P.<|> Just <$> el
    nul = P.oneOf "Nn" >> P.oneOf "Uu" >> P.oneOf "Ll" >> P.oneOf "Ll"
    el = pgDecodeBS . LC.pack <$> (qel P.<|> uqel)
    qel = P.between (P.char '"') (P.char '"') $
      P.many $ (P.char '\\' >> P.anyChar) P.<|> P.noneOf "\\\""
    uqel = P.many1 (P.noneOf "\",{}")
  pgEncode l = '{' : intercalate "," (map el l) ++ "}" where
    el Nothing = "null"
    el (Just e) = '"' : es (pgEncode e) -- quoting may not be necessary but is always safe
    es "" = "\""
    es (c@'"':r) = '\\':c:es r
    es (c@'\\':r) = '\\':c:es r
    es (c:r) = c:es r

{-
-- Since PG values cannot contain '\0', we use it as a special flag for NULL values (which will later be encoded with length -1)
pgNull :: String
pgNull = "\0"
pgNullBS :: L.ByteString
pgNullBS = L.singleton 0

-- This is a nice idea, but isn't actually useful because these types will never be resolved
instance PGType a => PGType (Maybe a) where
  pgDecodeBS s = pgDecodeBS s <$ guard (s /= pgNullBS)
  pgDecode s = pgDecode s <$ guard (s /= pgNull)
  pgEncodeBS = maybe pgNullBS pgEncodeBS
  pgEncode = maybe pgNull pgEncode
  pgLiteral = maybe "NULL" pgLiteral
-}

-- |A special class inhabited only by @a@ and @Maybe a@.
-- This is used to provide added flexibility in parameter types.
class PGType a => PossiblyMaybe m a {- ideally should have fundep: | m -> a -} where
  possiblyMaybe :: m -> Maybe a
  maybePossibly :: Maybe a -> m

instance PGType a => PossiblyMaybe a a where
  possiblyMaybe = Just
  maybePossibly = fromMaybe (error "Unexpected NULL value")
instance PGType a => PossiblyMaybe (Maybe a) a where
  possiblyMaybe = id
  maybePossibly = id

data PGTypeHandler = PGType
  { pgTypeName :: String -- ^ The internal PostgreSQL name of the type
  , pgTypeType :: TH.Type -- ^ The equivalent Haskell type to which it is marshalled (must be an instance of 'PGType'
  } deriving (Show)

-- |TH expression to decode a 'L.ByteString' to a value.
pgTypeDecoder :: PGTypeHandler -> TH.ExpQ
pgTypeDecoder PGType{ pgTypeType = t } =
  [| pgDecodeBS :: L.ByteString -> $(return t) |]

-- |TH expression to encode a ('PossiblyMayble') value to an 'Maybe' 'L.ByteString'.
pgTypeEncoder :: PGTypeHandler -> TH.ExpQ
pgTypeEncoder PGType{ pgTypeType = t } =
  [| fmap (pgEncodeBS :: $(return t) -> L.ByteString) . possiblyMaybe |]

-- |TH expression to escape a ('PossiblyMaybe') value to a SQL literal.
pgTypeEscaper :: PGTypeHandler -> TH.ExpQ
pgTypeEscaper PGType{ pgTypeType = t } =
  [| maybe "NULL" (pgLiteral :: $(return t) -> String) . possiblyMaybe |]

type PGTypeMap = Map.Map OID PGTypeHandler

arrayType :: TH.Type -> TH.Type
arrayType = TH.AppT TH.ListT . TH.AppT (TH.ConT ''Maybe)

pgArrayType :: String -> TH.Type -> PGTypeHandler
pgArrayType n t = PGType ('_':n) (arrayType t)

pgTypes :: [(OID, OID, String, TH.Name)]
pgTypes =
  [ (  16, 1000, "bool",        ''Bool)
  , (  17, 1001, "bytea",       ''L.ByteString)
  , (  18, 1002, "char",        ''Char)
  , (  19, 1003, "name",        ''String) -- limit 63 characters
  , (  20, 1016, "int8",        ''Int64)
  , (  21, 1005, "int2",        ''Int16)
  , (  23, 1007, "int4",        ''Int32)
  , (  25, 1009, "text",        ''String)
  , (  26, 1028, "oid",         ''OID)
--, ( 114,  199, "json",        ?)
--, ( 142,  143, "xml",         ?)
--, ( 600, 1017, "point",       ?)
--, ( 650,  651, "cidr",        ?)
  , ( 700, 1021, "float4",      ''Float)
  , ( 701, 1022, "float8",      ''Double)
--, ( 790,  791, "money",       Centi? Fixed?)
--, ( 829, 1040, "macaddr",     ?)
--, ( 869, 1041, "inet",        ?)
  , (1042, 1014, "bpchar",      ''String)
  , (1043, 1015, "varchar",     ''String)
  , (1082, 1182, "date",        ''Time.Day)
  , (1083, 1183, "time",        ''Time.TimeOfDay)
  , (1114, 1115, "timestamp",   ''Time.LocalTime)
  , (1184, 1185, "timestamptz", ''Time.ZonedTime)
  , (1186, 1187, "interval",    ''Time.DiffTime)
--, (1266, 1270, "timetz",      ?)
--, (1560, 1561, "bit",         Bool?)
--, (1562, 1563, "varbit",      ?)
  , (1700, 1231, "numeric",     ''Rational)
--, (2950, 2951, "uuid",        ?)
  ]

defaultTypeMap :: PGTypeMap
defaultTypeMap = Map.fromAscList [(o, PGType n (TH.ConT t)) | (o, _, n, t) <- pgTypes]
   `Map.union` Map.fromList [(o, pgArrayType n (TH.ConT t)) | (_, o, n, t) <- pgTypes]
