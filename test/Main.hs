module Main (main) where

import Database.TemplatePG
import System.Environment (setEnv)
import System.Exit (exitSuccess, exitFailure)

import Connect

assert :: Bool -> IO ()
assert False = exitFailure
assert True = return ()

useTHConnection connect

main :: IO ()
main = do
  c <- connect
  Just (Just 1) <- $(queryTuple "SELECT 1") c
  exitSuccess
