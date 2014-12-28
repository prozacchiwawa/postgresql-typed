module Main (main) where

import qualified Data.Time as Time
import Data.Int (Int32)
import System.Environment (setEnv)
import System.Exit (exitSuccess, exitFailure)

import Database.TemplatePG
import Connect

assert :: Bool -> IO ()
assert False = exitFailure
assert True = return ()

useTHConnection connect

main :: IO ()
main = do
  c <- connect
  _ <- $(queryTuples "SELECT oid, typname from pg_type") c
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
  exitSuccess
