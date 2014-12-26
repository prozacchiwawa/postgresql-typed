module Main (main) where

import qualified Data.Time as Time
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
  z <- Time.getZonedTime
  let t = Time.zonedTimeToLocalTime z
      d = Time.localDay t
      p = -34881559
  Just (Just 1, Just True, Just 3.14, Just d', Just t', Just z', Just p') <-
    $(queryTuple "SELECT {1}::int, {True}::bool, {3.14}::float4, {d}::date, {t}::timestamp, {z}::timestamptz, {p}::interval") c
  assert $ d == d' && t == t' && Time.zonedTimeToUTC z == Time.zonedTimeToUTC z' && p == p'
  exitSuccess
