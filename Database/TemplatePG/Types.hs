{-# LANGUAGE ExistentialQuantification, TypeSynonymInstances, FlexibleInstances #-}
-- Copyright 2010, 2011, 2013 Chris Forno
-- Copyright 2014 Dylan Simon

module Database.TemplatePG.Types 
  ( pgQuote
  , PGType(..)
  , OID
  , PGTypeHandler(..)
  , pgTypeDecoder
  , pgTypeEscaper
  , PGTypeMap
  , defaultTypeMap
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
import qualified Data.Map as Map
import Data.Maybe (fromMaybe)
import Data.Ratio ((%), numerator, denominator)
import qualified Data.Time as Time
import Data.Word (Word32)
import Language.Haskell.TH
import Numeric (readFloat)
import System.Locale (defaultTimeLocale)
import qualified Text.Parsec as P
import Text.Parsec.Token (naturalOrFloat, makeTokenParser, GenLanguageDef(..))
import Text.Read (readMaybe)

pgQuoteUnsafe :: String -> String
pgQuoteUnsafe s = '\'' : s ++ "'"

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
  pgDecodeBS :: L.ByteString -> Maybe a
  pgDecodeBS = pgDecode . U.toString
  -- |Decode a postgres unicode string representation into a value.
  pgDecode :: String -> Maybe a
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
  pgDecode "f" = return False
  pgDecode "t" = return True
  pgDecode _ = fail "bool"
  pgEncode False = "f"
  pgEncode True = "t"
  pgLiteral False = "false"
  pgLiteral True = "true"

type OID = Word32
instance PGType OID where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = readMaybe
  pgEncode = show
  pgLiteral = show

instance PGType Int where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = readMaybe
  pgEncode = show
  pgLiteral = show

instance PGType Int16 where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = readMaybe
  pgEncode = show
  pgLiteral = show

instance PGType Int32 where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = readMaybe
  pgEncode = show
  pgLiteral = show

instance PGType Int64 where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = readMaybe
  pgEncode = show
  pgLiteral = show

instance PGType Char where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode [c] = return c
  pgDecode _ = fail "char"
  pgEncode c
    | fromEnum c < 256 = [c]
    | otherwise = error "pgEncode: Char out of range"

instance PGType Float where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = readMaybe
  pgEncode = show
  pgLiteral = show

instance PGType Double where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = readMaybe
  pgEncode = show
  pgLiteral = show

instance PGType String where
  pgDecode = return
  pgEncode = id

type Bytea = L.ByteString
instance PGType Bytea where
  pgDecode = pgDecodeBS . LC.pack
  pgEncodeBS = LC.pack . pgEncode
  pgDecodeBS s
    | LC.unpack m /= "\\x" = fail "bytea"
    | otherwise = return $ L.pack $ pd $ L.unpack d where
    (m, d) = L.splitAt 2 s
    pd [] = []
    pd (h:l:r) = (shiftL (unhex h) 4 .|. unhex l) : pd r
    pd [x] = error $ "parseBytea: " ++ show x
    unhex = fromIntegral . digitToInt . w2c
  pgEncode = (++) "'\\x" . ed . L.unpack where
    ed [] = "\'"
    ed (x:d) = hex (shiftR x 4) : hex (x .&. 0xF) : ed d
    hex = intToDigit . fromIntegral
  pgLiteral = pgQuoteUnsafe . pgEncode

instance PGType Time.Day where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = Time.parseTime defaultTimeLocale "%F"
  pgEncode = Time.showGregorian
  pgLiteral = pgQuoteUnsafe . pgEncode

instance PGType Time.TimeOfDay where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = Time.parseTime defaultTimeLocale "%T%Q"
  pgEncode = Time.formatTime defaultTimeLocale "%T%Q"
  pgLiteral = pgQuoteUnsafe . pgEncode

instance PGType Time.LocalTime where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = Time.parseTime defaultTimeLocale "%F %T%Q"
  pgEncode = Time.formatTime defaultTimeLocale "%F %T%Q"
  pgLiteral = pgQuoteUnsafe . pgEncode

instance PGType Time.ZonedTime where
  pgDecodeBS = pgDecode . LC.unpack
  pgEncodeBS = LC.pack . pgEncode
  pgDecode = Time.parseTime defaultTimeLocale "%F %T%Q%z" . fixTZ
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
  pgDecodeBS = either (fail . show) return . P.parse ps "interval" where
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
  pgDecode "NaN" = Just (0 % 0) -- this won't work
  pgDecode s = unReads $ readFloat s
  pgEncode r
    | denominator r == 0 = "NaN" -- this can't happen
    | otherwise = take 30 (showRational (r / (10 ^^ e))) ++ 'e' : show e where
    e = floor $ logBase 10 $ fromRational $ abs r -- not great, and arbitrarily truncate somewhere
  pgLiteral r
    | denominator r == 0 = "'NaN'" -- this can't happen
    | otherwise = '(' : show (numerator r) ++ '/' : show (denominator r) ++ "::numeric)"

-- This may produce infinite strings
showRational :: Rational -> String
showRational r = show (ri :: Integer) ++ '.' : frac rf where
  (ri, rf) = properFraction r
  frac 0 = ""
  frac f = intToDigit i : frac f' where (i, f') = properFraction (10 * f)

unReads :: [(a,String)] -> Maybe a
unReads [(x,"")] = return x
unReads _ = fail "unReads: no parse"


data PGTypeHandler = PGType
  { pgTypeName :: String
  , pgTypeType :: Type
  }

pgTypeDecoder :: PGTypeHandler -> Q Exp
pgTypeDecoder PGType{ pgTypeType = t } =
  [| fromMaybe (error "pgDecode: no parse") . pgDecodeBS :: L.ByteString -> $(return t) |]

pgTypeEscaper :: PGTypeHandler -> Q Exp
pgTypeEscaper PGType{ pgTypeType = t } =
  [| pgLiteral :: $(return t) -> String |]

type PGTypeMap = Map.Map OID PGTypeHandler

defaultTypeMap :: PGTypeMap
defaultTypeMap = Map.fromAscList
  [ (16, PGType "bool" (ConT ''Bool))
  , (17, PGType "bytea" (ConT ''L.ByteString))
  , (18, PGType "char" (ConT ''Char))
  -- , (19, PGType "name")
  , (20, PGType "int8" (ConT ''Int64))
  , (21, PGType "int2" (ConT ''Int16))
  , (23, PGType "int4" (ConT ''Int32))
  , (25, PGType "text" (ConT ''String))
  , (26, PGType "oid" (ConT ''OID))
  , (700, PGType "float4" (ConT ''Float))
  , (701, PGType "float8" (ConT ''Double))
  , (1042, PGType "bpchar" (ConT ''String))
  , (1043, PGType "varchar" (ConT ''String))
  , (1082, PGType "date" (ConT ''Time.Day))
  , (1083, PGType "time" (ConT ''Time.TimeOfDay))
  , (1114, PGType "timestamp" (ConT ''Time.LocalTime))
  , (1184, PGType "timestamptz" (ConT ''Time.ZonedTime))
  , (1186, PGType "interval" (ConT ''Time.DiffTime))
  -- , (1560, PGType "bit")
  -- , (1562, PGType "varbit")
  , (1700, PGType "numeric" (ConT ''Rational))
  ]
