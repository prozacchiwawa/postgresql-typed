{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses, DataKinds #-}
-- {-# OPTIONS_GHC -ddump-splices #-}
module Main (main) where

import Data.Int (Int32)
import qualified Data.Time as Time
import System.Exit (exitSuccess, exitFailure)

import Database.PostgreSQL.Typed
import Database.PostgreSQL.Typed.Types (OID)
import qualified Database.PostgreSQL.Typed.Range as Range

import Connect

assert :: Bool -> IO ()
assert False = exitFailure
assert True = return ()

useTPGDatabase db

simple :: PGConnection -> OID -> IO [String]
simple c t = pgQuery c [pgSQL|SELECT typname FROM pg_catalog.pg_type WHERE oid = ${t} AND oid = $1|]
simpleApply :: PGConnection -> OID -> IO [Maybe String]
simpleApply c = pgQuery c . [pgSQL|?SELECT typname FROM pg_catalog.pg_type WHERE oid = $1|]
prepared :: PGConnection -> OID -> String -> IO [Maybe String]
prepared c t = pgQuery c . [pgSQL|?$SELECT typname FROM pg_catalog.pg_type WHERE oid = ${t} AND typname = $2|]
preparedApply :: PGConnection -> Int32 -> IO [String]
preparedApply c = pgQuery c . [pgSQL|$(integer)SELECT typname FROM pg_catalog.pg_type WHERE oid = $1|]

main :: IO ()
main = do
  c <- pgConnect db
  z <- Time.getZonedTime
  let i = 1 :: Int32
      b = True
      f = 3.14 :: Float
      t = Time.zonedTimeToLocalTime z
      d = Time.localDay t
      p = -34881559 :: Time.DiffTime
      s = "\"hel\\o'"
      l = [Just "a\\\"b,c", Nothing]
      r = Range.normal (Just (-2 :: Int32)) Nothing
  [(Just b', Just i', Just f', Just s', Just d', Just t', Just z', Just p', Just l', Just r')] <- pgQuery c
    [pgSQL|$SELECT ${b}::bool, ${Just i}::int, ${f}::float4, ${s}::varchar(10), ${Just d}::date, ${t}::timestamp, ${Time.zonedTimeToUTC z}::timestamptz, ${p}::interval, ${l}::text[], ${r}::int4range|]
  assert $ i == i' && b == b' && s == s' && f == f' && d == d' && t == t' && Time.zonedTimeToUTC z == z' && p == p' && l == l' && r == r'

  ["box"] <- simple c 603
  [Just "box"] <- simpleApply c 603
  [Just "box"] <- prepared c 603 "box"
  ["box"] <- preparedApply c 603
  [Just "line"] <- prepared c 628 "line"
  ["line"] <- preparedApply c 628

  pgDisconnect c
  exitSuccess
