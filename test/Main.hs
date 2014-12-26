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
  t <- Time.getZonedTime
  let d = Time.localDay $ Time.zonedTimeToLocalTime t
      p = -34881559
  Just (Just 1, Just True, Just 3.14, Just d', Just t', Just p') <-
    $(queryTuple "SELECT {1}::int, {True}::bool, {3.14}::float4, {d}::date, {t}::timestamptz, {p}::interval") c
  assert $ d == d' && Time.zonedTimeToUTC t == Time.zonedTimeToUTC t' && p == p'
  exitSuccess
