module Main (main) where

import Database.TemplatePG
import Database.TemplatePG.SQL (thConnection)
import Network (PortID(UnixSocket))
import System.Environment (setEnv)
import System.Exit (exitSuccess, exitFailure)

assert :: Bool -> IO ()
assert False = exitFailure
assert True = return ()

main :: IO ()
main = do
  h <- thConnection -- just to use the same connection parameters, not best practice
  Just (Just 1) <- $(queryTuple "SELECT 1") h
  exitSuccess
