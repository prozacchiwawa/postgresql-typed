module Main (main) where

import qualified Data.Time as Time
import Data.Int (Int32)
import System.Environment (setEnv)
import System.Exit (exitSuccess, exitFailure)

import Database.TemplatePG
import Database.TemplatePG.Types (OID)
import Connect

assert :: Bool -> IO ()
assert False = exitFailure
assert True = return ()

useTHConnection connect

simple, simpleApply, prepared, preparedApply :: PGConnection -> OID -> IO [String]
simple        c t = pgQuery c $(makePGSimpleQuery   "SELECT typname FROM pg_catalog.pg_type WHERE oid = ${t} AND oid = $1")
simpleApply   c = pgQuery c . $(makePGSimpleQuery   "SELECT typname FROM pg_catalog.pg_type WHERE oid = $1")
prepared      c t = pgQuery c $(makePGPreparedQuery "SELECT typname FROM pg_catalog.pg_type WHERE oid = ${t} AND oid = $1")
preparedApply c = pgQuery c . $(makePGPreparedQuery "SELECT typname FROM pg_catalog.pg_type WHERE oid = $1")

main :: IO ()
main = do
  c <- connect
  z <- Time.getZonedTime
  let i = 1
      b = True
      f = 3.14
      t = Time.zonedTimeToLocalTime z
      d = Time.localDay t
      p = -34881559
      l = [Just "a\\\"b,c", Nothing]
  Just (Just i', Just b', Just f', Just d', Just t', Just z', Just p', Just l') <-
    $(queryTuple "SELECT {Just i}::int, {b}::bool, {f}::float4, {Just d}::date, {t}::timestamp, {z}::timestamptz, {p}::interval, {l}::text[]") c
  assert $ i == i' && b == b' && f == f' && d == d' && t == t' && Time.zonedTimeToUTC z == Time.zonedTimeToUTC z' && p == p' && l == l'

  ["box"] <- simple c 603
  ["box"] <- simpleApply c 603
  ["box"] <- prepared c 603
  ["box"] <- preparedApply c 603
  ["line"] <- prepared c 628
  ["line"] <- preparedApply c 628

  pgDisconnect c
  exitSuccess
