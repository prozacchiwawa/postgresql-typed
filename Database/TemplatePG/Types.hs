{-# LANGUAGE ExistentialQuantification #-}
-- Copyright 2010, 2011, 2013 Chris Forno
-- Copyright 2014 Dylan Simon

module Database.TemplatePG.Types where

import Control.Applicative ((<$>), (<$))
import Control.Monad (mzero)
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.Char8 as LC
import qualified Data.ByteString.Lazy.UTF8 as U
import Data.Char (isDigit)
import Data.Int
import qualified Data.Map as Map
import qualified Data.Time as Time
import Data.Word (Word32)
import Language.Haskell.TH
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
      [|| parseBool . LC.unpack ||]
      [|| \b -> if b then "true" else "false" ||])
  -- , (17, PGType "bytea")
  , (18, PGType "char" (ConT ''Char)
      [|| LC.head ||]
      [|| escapeChar ||])
  -- , (19, PGType "name")
  , (20, mkPGLit "int8" (ConT ''Int64) (0 :: Int64))
  , (21, mkPGLit "int2" (ConT ''Int16) (0 :: Int16))
  -- , (22, PGType "int2vector")
  , (23, mkPGLit "int4" (ConT ''Int32) (0 :: Int32))
  , (25, PGType "text" (ConT ''String)
      [|| U.toString ||]
      [|| escapeString ||])
  , (26, mkPGLit "oid" (ConT ''OID) (0 :: OID))
  , (700, mkPGLit "float4" (ConT ''Float) (0 :: Float))
  , (701, mkPGLit "float8" (ConT ''Float) (0 :: Double))
  -- , (1042, PGType "bpchar"
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
  -- , (1560, PGType "bit"
  -- , (1562, PGType "varbit"
  -- , (1700, PGType "numeric"
  ]

parseBool :: String -> Bool
parseBool "f" = False
parseBool "t" = True
parseBool b = error $ "parseBool: " ++ b

escapeChar :: Char -> String
escapeChar '\'' = "''"
escapeChar c = return c

escapeString :: String -> String
escapeString = ('\'' :) . es where -- concatMap escapeChar
  es "" = "'"
  es (c@'\'':s) = c:c:es s
  es (c:s) = c:es s

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
