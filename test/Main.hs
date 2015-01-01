module Main (main) where

import qualified Data.Time as Time
import Data.Int (Int32)
import System.Environment (setEnv)
import System.Exit (exitSuccess, exitFailure)

import Database.TemplatePG
import Database.TemplatePG.Types (OID)
import Database.TemplatePG.SQL
import qualified Database.TemplatePG.Range as Range
import Connect

assert :: Bool -> IO ()
assert False = exitFailure
assert True = return ()

useTHConnection connect

simple :: PGConnection -> OID -> IO [String]
simple        c t = pgQuery c [pgSQL|SELECT typname FROM pg_catalog.pg_type WHERE oid = ${t} AND oid = $1|]
simpleApply :: PGConnection -> OID -> IO [Maybe String]
simpleApply   c = pgQuery c . [pgSQL|?SELECT typname FROM pg_catalog.pg_type WHERE oid = $1|]
prepared :: PGConnection -> OID -> IO [Maybe String]
prepared      c t = pgQuery c [pgSQL|?$SELECT typname FROM pg_catalog.pg_type WHERE oid = ${t} AND oid = $1|]
preparedApply :: PGConnection -> [pgSQL|int4|] -> IO [String]
preparedApply c = pgQuery c . [pgSQL|$(integer)SELECT typname FROM pg_catalog.pg_type WHERE oid = $1|]

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
      r = Range.normalRange (Just (-2)) Nothing
  Just (Just i', Just b', Just f', Just d', Just t', Just z', Just p', Just l', Just r') <-
    $(queryTuple "SELECT {Just i}::int, {b}::bool, {f}::float4, {Just d}::date, {t}::timestamp, {Time.zonedTimeToUTC z}::timestamptz, {p}::interval, {l}::text[], {r}::int4range") c
  assert $ i == i' && b == b' && f == f' && d == d' && t == t' && Time.zonedTimeToUTC z == z' && p == p' && l == l' && r == r'

  ["box"] <- simple c 603
  [Just "box"] <- simpleApply c 603
  [Just "box"] <- prepared c 603
  ["box"] <- preparedApply c 603
  [Just "line"] <- prepared c 628
  ["line"] <- preparedApply c 628

  pgDisconnect c
  exitSuccess
