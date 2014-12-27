{-# LANGUAGE ExistentialQuantification #-}
-- Copyright 2010, 2011, 2013 Chris Forno
-- Copyright 2014 Dylan Simon

module Database.TemplatePG.Types where

import Control.Applicative ((<$>), (<$))
import Control.Monad (mzero)
import Data.Bits (shiftL, shiftR, (.|.), (.&.))
import Data.ByteString.Internal (w2c)
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.Char8 as LC
import qualified Data.ByteString.Lazy.UTF8 as U
import Data.Char (isDigit)
import Data.Int
import qualified Data.Map as Map
import Data.Ratio (numerator, denominator)
import qualified Data.Time as Time
import Data.Word (Word32)
import Language.Haskell.TH
import Numeric (readFloat)
import System.Locale (defaultTimeLocale)
import qualified Text.Parsec as P
import Text.Parsec.Token (naturalOrFloat, makeTokenParser, GenLanguageDef(..))

type OID = Word32

data PGType = forall a . PGType
  { pgTypeName :: String
  , pgTypeType :: Type
  , pgTypeDecode :: Q (TExp (L.ByteString -> a))
  , pgTypeEscape :: Q (TExp (a -> String))
  }

pgTypeDecoder :: PGType -> Q Exp
pgTypeDecoder PGType{ pgTypeType = t, pgTypeDecode = f } =
  sigE (unType <$> f) $ return $ ArrowT `AppT` ConT ''L.ByteString `AppT` t

pgTypeEscaper :: PGType -> Q Exp
pgTypeEscaper PGType{ pgTypeType = t, pgTypeEscape = f } =
  sigE (unType <$> f) $ return $ ArrowT `AppT` t `AppT` ConT ''String

mkPGType :: String -> Type -> Q (TExp (L.ByteString -> a)) -> Q (TExp (a -> String)) -> a -> PGType
mkPGType name typ rd shw _ = PGType name typ rd shw

mkPGLit :: (Read a, Show a) => String -> Type -> a -> PGType
mkPGLit name typ = mkPGType name typ [|| read . LC.unpack ||] [|| show ||]

type PGTypeMap = Map.Map OID PGType

defaultTypeMap :: PGTypeMap
defaultTypeMap = Map.fromAscList
  [ (16, PGType "bool" (ConT ''Bool)
      [|| parseBool ||]
      [|| \b -> if b then "true" else "false" ||])
  , (17, PGType "bytea" (ConT ''L.ByteString)
      [|| parseBytea ||]
      [|| escapeBytea ||])
  , (18, PGType "char" (ConT ''Char)
      [|| LC.head ||]
      [|| escapeChar ||])
  -- , (19, PGType "name")
  , (20, mkPGLit "int8" (ConT ''Int64) (0 :: Int64))
  , (21, mkPGLit "int2" (ConT ''Int16) (0 :: Int16))
  , (23, mkPGLit "int4" (ConT ''Int32) (0 :: Int32))
  , (25, PGType "text" (ConT ''String)
      [|| U.toString ||]
      [|| escapeString ||])
  , (26, mkPGLit "oid" (ConT ''OID) (0 :: OID))
  , (700, mkPGLit "float4" (ConT ''Float) (0 :: Float))
  , (701, mkPGLit "float8" (ConT ''Float) (0 :: Double))
  -- , (1042, PGType "bpchar")
  , (1043, PGType "varchar" (ConT ''String)
      [|| U.toString ||]
      [|| escapeString ||])
  , (1082, mkPGType "date" (ConT ''Time.Day)
      [|| Time.readTime defaultTimeLocale "%F" . LC.unpack ||]
      [|| escapeString . Time.showGregorian ||]
      (undefined :: Time.Day))
  , (1083, mkPGType "time" (ConT ''Time.TimeOfDay)
      [|| Time.readTime defaultTimeLocale "%T%Q" . LC.unpack ||]
      [|| escapeString . Time.formatTime defaultTimeLocale "%T%Q" ||]
      (undefined :: Time.TimeOfDay))
  , (1114, mkPGType "timestamp" (ConT ''Time.LocalTime)
      [|| Time.readTime defaultTimeLocale "%F %T%Q" . LC.unpack ||]
      [|| escapeString . Time.formatTime defaultTimeLocale "%F %T%Q" ||]
      (undefined :: Time.LocalTime))
  , (1184, mkPGType "timestamptz" (ConT ''Time.ZonedTime)
      [|| Time.readTime defaultTimeLocale "%F %T%Q%z" . fixTZ . LC.unpack ||]
      [|| escapeString . fixTZ . Time.formatTime defaultTimeLocale "%F %T%Q%z" ||]
      (undefined :: Time.ZonedTime))
  , (1186, PGType "interval" (ConT ''Time.DiffTime)
      [|| parseInterval ||]
      [|| escapeString . show ||])
  -- , (1560, PGType "bit")
  -- , (1562, PGType "varbit")
  , (1700, mkPGType "numeric" (ConT ''Rational)
      [|| unReads readFloat . LC.unpack ||]
      [|| escapeRational ||]
      (0 :: Rational))
  ]

unReads :: ReadS a -> String -> a
unReads r = ur . r where
  ur [(x,"")] = x
  ur _ = error "unReads: no parse"

parseBool :: L.ByteString -> Bool
parseBool = pb . LC.unpack where
  pb "f" = False
  pb "t" = True
  pb b = error $ "parseBool: " ++ b

escapeChar :: Char -> String
escapeChar '\'' = "''"
escapeChar c = return c

escapeString :: String -> String
escapeString = ('\'' :) . es where -- concatMap escapeChar
  es "" = "'"
  es (c@'\'':s) = c:c:es s
  es (c:s) = c:es s

parseBytea :: L.ByteString -> L.ByteString
parseBytea s 
  | LC.unpack m /= "\\x" = error $ "parseBytea: " ++ LC.unpack m
  | otherwise = L.pack $ pd $ L.unpack d where
  (m, d) = L.splitAt 2 s
  pd [] = []
  pd (h:l:r) = (shiftL (unhex h) 4 .|. unhex l) : pd r
  pd [x] = error $ "parseBytea: " ++ show x
  unhex c
    | c >= 48 && c <= 57 = c - 48
    | c >= 65 && c <= 70 = c - 55
    | c >= 97 && c <= 102 = c - 87
    | otherwise = error $ "parseBytea: " ++ show c

escapeBytea :: L.ByteString -> String
escapeBytea = (++) "'\\x" . ed . L.unpack where
  ed [] = "\'"
  ed (x:d) = hex (shiftR x 4) : hex (x .&. 15) : ed d
  hex c
    | c < 10 = w2c $ 48 + c
    | otherwise = w2c $ 87 + c

escapeRational :: Rational -> String
escapeRational r = '(' : show (numerator r) ++ '/' : show (denominator r) ++ "::numeric)"

-- PostgreSQL uses "[+-]HH[:MM]" timezone offsets, while "%z" uses "+HHMM" by default.
-- readTime can successfully parse both formats, but PostgreSQL needs the colon.
fixTZ :: String -> String
fixTZ "" = ""
fixTZ ['+',h1,h2] | isDigit h1 && isDigit h2 = ['+',h1,h2,':','0','0']
fixTZ ['-',h1,h2] | isDigit h1 && isDigit h2 = ['-',h1,h2,':','0','0']
fixTZ ['+',h1,h2,m1,m2] | isDigit h1 && isDigit h2 && isDigit m1 && isDigit m2 = ['+',h1,h2,':',m1,m2]
fixTZ ['-',h1,h2,m1,m2] | isDigit h1 && isDigit h2 && isDigit m1 && isDigit m2 = ['-',h1,h2,':',m1,m2]
fixTZ (c:s) = c:fixTZ s

-- PostgreSQL stores months and days separately, but here we must collapse them into seconds
parseInterval :: L.ByteString -> Time.DiffTime
parseInterval = either (error . show) id . P.parse ps "interval" where
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
