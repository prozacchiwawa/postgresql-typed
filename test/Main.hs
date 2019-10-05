{-# LANGUAGE OverloadedStrings, FlexibleInstances, MultiParamTypeClasses, DataKinds, DeriveDataTypeable, TypeFamilies, PatternGuards, StandaloneDeriving #-}
{-# LANGUAGE ViewPatterns #-}
{-# OPTIONS_GHC -fno-warn-orphans -Wincomplete-uni-patterns #-}
--{-# OPTIONS_GHC -ddump-splices #-}
module Main (main) where

import Control.Exception (try)
import Control.Monad (unless)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC
import Data.Char (isDigit, toUpper)
import Data.Int (Int32)
import qualified Data.Time as Time
import qualified Data.Vector as V
import Data.Word (Word8)
import System.Exit (exitSuccess, exitFailure)
import qualified Test.QuickCheck as Q
import Test.QuickCheck.Test (isSuccess)

import Database.PostgreSQL.Typed
import Database.PostgreSQL.Typed.Types
import Database.PostgreSQL.Typed.Protocol
import Database.PostgreSQL.Typed.Query (PGSimpleQuery, getQueryString)
import Database.PostgreSQL.Typed.Array ()
import qualified Database.PostgreSQL.Typed.Range as Range
import Database.PostgreSQL.Typed.Enum
import Database.PostgreSQL.Typed.Inet
import Database.PostgreSQL.Typed.SQLToken
import Database.PostgreSQL.Typed.Relation
import qualified Database.PostgreSQL.Typed.ErrCodes as PGErr

import Connect

assert :: Bool -> IO ()
assert False = exitFailure
assert True = return ()

useTPGDatabase db

-- This runs at compile-time:
[pgSQL|!CREATE TYPE myenum AS enum ('abc', 'DEF', 'XX_ye')|]

[pgSQL|!CREATE TABLE myfoo (id serial primary key, adx myenum, bar char(4))|]

dataPGEnum "MyEnum" "myenum" ("MyEnum_" ++)

deriving instance Show MyEnum

dataPGRelation "MyFoo" "myfoo" (\(c:s) -> "foo" ++ toUpper c : s)

_fooRow :: MyFoo
_fooRow = MyFoo{ fooId = 1, fooAdx = Just MyEnum_DEF, fooBar = Just "abcd" }

instance Q.Arbitrary MyEnum where
  arbitrary = Q.arbitraryBoundedEnum

instance Q.Arbitrary Time.Day where
  arbitrary = Time.ModifiedJulianDay <$> Q.arbitrary
instance Q.Arbitrary Time.DiffTime where
  arbitrary = Time.picosecondsToDiffTime . (1000000 *) <$> Q.arbitrary
instance Q.Arbitrary Time.UTCTime where
  arbitrary = Time.UTCTime <$> Q.arbitrary <*> ((Time.picosecondsToDiffTime . (1000000 *)) <$> Q.choose (0,86399999999))
instance Q.Arbitrary Time.LocalTime where
  arbitrary = Time.utcToLocalTime Time.utc <$> Q.arbitrary

instance Q.Arbitrary a => Q.Arbitrary (Range.Bound a) where
  arbitrary = do
    u <- Q.arbitrary
    if u
      then return $ Range.Unbounded
      else Range.Bounded <$> Q.arbitrary <*> Q.arbitrary
instance (Ord a, Q.Arbitrary a) => Q.Arbitrary (Range.Range a) where
  arbitrary = Range.range <$> Q.arbitrary <*> Q.arbitrary

instance Q.Arbitrary PGInet where
  arbitrary = do
    v6 <- Q.arbitrary
    if v6
      then PGInet6 <$> Q.arbitrary <*> ((`mod` 129) <$> Q.arbitrary)
      else PGInet  <$> Q.arbitrary <*> ((`mod`  33) <$> Q.arbitrary)

instance Q.Arbitrary SQLToken where
  arbitrary = Q.oneof
    [ SQLToken <$> Q.arbitrary
    , SQLParam <$> Q.arbitrary
    , SQLExpr <$> Q.arbitrary
    , SQLQMark <$> Q.arbitrary
    ]
    
newtype Str = Str { strString :: [Char] } deriving (Eq, Show)
strByte :: Str -> BS.ByteString
strByte = BSC.pack . strString
byteStr :: BS.ByteString -> Str
byteStr = Str . BSC.unpack
instance Q.Arbitrary Str where
  arbitrary = Str <$> Q.listOf (Q.choose (' ', '~'))

simple :: PGConnection -> OID -> IO [String]
simple c t = pgQuery c [pgSQL|SELECT typname FROM pg_catalog.pg_type WHERE oid = ${t} AND oid = $1|]
simpleApply :: PGConnection -> OID -> IO [Maybe String]
simpleApply c = pgQuery c . [pgSQL|?SELECT typname FROM pg_catalog.pg_type WHERE oid = $1|]
prepared :: PGConnection -> OID -> String -> IO [Maybe String]
prepared c t = pgQuery c . [pgSQL|?$SELECT typname FROM pg_catalog.pg_type WHERE oid = ${t} AND typname = $2|]
preparedApply :: PGConnection -> Int32 -> IO [String]
preparedApply c = pgQuery c . [pgSQL|$(integer)SELECT typname FROM pg_catalog.pg_type WHERE oid = $1|]

selectProp :: PGConnection -> Bool -> Word8 -> Int32 -> Float -> Time.LocalTime -> Time.UTCTime -> Time.Day -> Time.DiffTime -> Str -> [Maybe Str] -> Range.Range Int32 -> MyEnum -> PGInet -> Q.Property
selectProp pgc b c i f t z d p s l r e a = Q.ioProperty $ do
  [(Just b', Just c', Just i', Just f', Just s', Just d', Just t', Just z', Just p', Just l', Just r', Just e', Just a')] <- pgQuery pgc
    [pgSQL|$SELECT ${b}::bool, ${c}::"char", ${Just i}::int, ${f}::float4, ${strString s}::varchar, ${Just d}::date, ${t}::timestamp, ${z}::timestamptz, ${p}::interval, ${map (fmap strByte) l}::text[], ${r}::int4range, ${e}::myenum, ${a}::inet|]
  return $ Q.conjoin 
    [ i Q.=== i'
    , c Q.=== c'
    , b Q.=== b'
    , strString s Q.=== s'
    , f Q.=== f'
    , d Q.=== d'
    , t Q.=== t'
    , z Q.=== z'
    , p Q.=== p'
    , l Q.=== map (fmap byteStr) l'
    , Range.normalize' r Q.=== r'
    , e Q.=== e'
    , a Q.=== a'
    ]

tokenProp :: String -> Q.Property
tokenProp s =
  not (has0 s) Q.==> s Q.=== show (sqlTokens s) where
  has0 ('$':'0':c:_) | isDigit c = True
  has0 (_:r) = has0 r
  has0 [] = False

main :: IO ()
main = do
  c <- pgConnect db

  r <- Q.quickCheckResult
    $ selectProp c
    Q..&&. tokenProp
    Q..&&. [pgSQL|#abc ${3.14::Float} def $f$ $$ ${1} $f$${2::Int32}|] Q.=== "abc 3.14::real def $f$ $$ ${1} $f$2::integer"
    Q..&&. getQueryString c ([pgSQL|SELECT ${"ab'cd"::String}::text, ${3.14::Float}::float4|] :: PGSimpleQuery (Maybe String, Maybe Float)) Q.=== "SELECT 'ab''cd'::text, 3.14::float4"
    Q..&&. pgEnumValues Q.=== [(MyEnum_abc, "abc"), (MyEnum_DEF, "DEF"), (MyEnum_XX_ye, "XX_ye")]
    Q..&&. Q.conjoin (map (\(s, t) -> sqlTokens s Q.=== t)
      [ ("",
        [])
      , (  "SELECT a from b WHERE c = ?"
        , ["SELECT a from b WHERE c = ", SQLQMark False])
      , (  "INSERT INTO foo VALUES (?,?)"
        , ["INSERT INTO foo VALUES (", SQLQMark False, ",", SQLQMark False, ")"])
      , (  "INSERT INTO foo VALUES ('?','''?')"
        , ["INSERT INTO foo VALUES ('?','''?')"])
      , (  "-- really?\n-- yes'?\nINSERT INTO ? VALUES ('', ?, \"?asd\", e'?\\'?', '?''?', /* foo? */ /* foo /* bar */ ? */ ?)"
        , ["-- really?\n-- yes'?\nINSERT INTO ", SQLQMark False, " VALUES ('', ", SQLQMark False, ", \"?asd\", e'?\\'?', '?''?', /* foo? */ /* foo /* bar */ ? */ ", SQLQMark False, ")"])
        , (  "some ${things? {don't}} change$1 $1\\?"
          , ["some ", SQLExpr "things? {don't}", " change$1 ", SQLParam 1, SQLQMark True])
      ])
  assert $ isSuccess r

  ["box"] <- simple c 603
  [Just "box"] <- simpleApply c 603
  [Just "box"] <- prepared c 603 "box"
  ["box"] <- preparedApply c 603
  [Just "line"] <- prepared c 628 "line"
  ["line"] <- preparedApply c 628

  pgSimpleQueries_ c "LISTEN channame; NOTIFY channame, 'oh hello'; SELECT pg_notify('channame', 'there')"
  PGNotification _ "channame" "oh hello" <- pgGetNotification c
  (-1, []) <- pgSimpleQuery c "NOTIFY channame"

  pgTransaction c $ do
    (1, [V.toList -> [PGTextValue "1"]]) <- pgSimpleQuery c "SELECT 1"
    (-1, []) <- pgSimpleQuery c "NOTIFY channame, 'nope'"
    Left e1 <- try $ pgSimpleQuery c "SYNTAX_ERROR"
    assert $ pgErrorCode e1 == PGErr.syntax_error
    Left e2 <- try $ pgSimpleQuery c "SELECT 1"
    assert $ pgErrorCode e2 == PGErr.in_failed_sql_transaction

  unless (pgSupportsTls c) $ do
    [PGNotification _ "channame" "there", PGNotification _ "channame" ""] <- pgGetNotifications c
    [] <- pgGetNotifications c
    pure ()

  pgDisconnect c
  exitSuccess
