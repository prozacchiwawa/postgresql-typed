{-# LANGUAGE ExistentialQuantification #-}
-- Copyright 2010, 2011, 2013 Chris Forno
-- Copyright 2014 Dylan Simon

module Database.TemplatePG.Types
  ( OID
  , PGType(..)
  , PGTypeMap
  , defaultTypeMap
  ) where

import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.Char8 as LC
import qualified Data.ByteString.Lazy.UTF8 as U
import Data.Int
import qualified Data.Map as Map
import Data.Word (Word32)
import Language.Haskell.TH

type OID = Word32

data PGType = forall a . PGType
  { pgTypeName :: String
  , pgTypeType :: Type
  , pgTypeDecode :: Q (TExp (L.ByteString -> a))
  , pgTypeShow :: Q (TExp (a -> String))
  }

mkPGType :: String -> Type -> Q (TExp (L.ByteString -> a)) -> Q (TExp (a -> String)) -> a -> PGType
mkPGType name typ rd shw _ = PGType name typ rd shw

mkPGLit :: (Read a, Show a) => String -> Type -> a -> PGType
mkPGLit name typ = mkPGType name typ [|| read . LC.unpack ||] [|| show ||]

type PGTypeMap = Map.Map OID PGType

defaultTypeMap :: PGTypeMap
defaultTypeMap = Map.fromAscList
  [ (16, PGType "bool" (ConT ''Bool)
      [|| readBool . LC.unpack ||]
      [|| \b -> if b then "t" else "f" ||])
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
  , (1043, PGType "varchar" (ConT ''String)
      [|| U.toString ||]
      [|| escapeString ||])
  -- , (1082, PGType "date")
  -- , (1184, PGType "timestamptz")
  -- , (1186, PGType "interval")
  ]

readBool :: String -> Bool
readBool "f" = False
readBool "t" = True
readBool b = error $ "readBool: " ++ b

escapeChar :: Char -> String
escapeChar '\'' = "''"
escapeChar c = return c

escapeString :: String -> String
escapeString s = '\'' : concatMap escapeChar s ++ "'"

{-
-- |This is PostgreSQL's canonical timestamp format.
-- Time conversions are complicated a bit because PostgreSQL doesn't support
-- timezones with minute parts, and Haskell only supports timezones with
-- minutes parts. We'll need to truncate and pad timestamp strings accordingly.
-- This means with minute parts will not work.
pgTimestampTZFormat :: String
pgTimestampTZFormat = "%F %T%z"

-- |Convert a Haskell value to a string of the given PostgreSQL type. Or, more
-- accurately, given a PostgreSQL type, create a function for converting
-- compatible Haskell values into a string of that type.
-- @pgTypeToString :: PGType -> (? -> String)@
pgTypeToString :: PGType -> Q Exp
pgTypeToString PGTimestampTZ = [| \t -> let ts = formatTime defaultTimeLocale pgTimestampTZFormat t in
                                        "TIMESTAMP WITH TIME ZONE '" ++
                                        (take (length ts - 2) ts) ++ "'" |]
pgTypeToString PGDate        = [| \d -> "'" ++ showGregorian d ++ "'" |]
pgTypeToString PGInterval    = [| \s -> "'" ++ show (s::DiffTime) ++ "'" |]

-- |Convert a string from PostgreSQL of the given type into an appropriate
-- Haskell value. Or, more accurately, given a PostgreSQL type, create a
-- function for converting a string of that type into a compatible Haskell
-- value.
-- @pgStringToType :: PGType -> (String -> ?)@
pgStringToType :: PGType -> Q Exp
-- TODO: Is reading to any integral type too unsafe to justify the convenience?
pgStringToType PGTimestampTZ = [| \t -> readTime defaultTimeLocale pgTimestampTZFormat (t ++ "00") |]
pgStringToType PGDate        = [| readTime defaultTimeLocale "%F" |]
pgStringToType PGInterval    = error "Reading PostgreSQL intervals isn't supported (yet)."
-}
