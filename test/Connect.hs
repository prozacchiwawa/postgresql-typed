{-# LANGUAGE CPP, OverloadedStrings #-}
module Connect where

import Data.Maybe (fromMaybe)
import Database.PostgreSQL.Typed (PGDatabase(..), defaultPGDatabase)
import Network.Socket (SockAddr(SockAddrUnix))
import System.Environment (lookupEnv)
import System.IO.Unsafe (unsafePerformIO)

db :: PGDatabase
db = defaultPGDatabase
  { pgDBName = "templatepg"
#ifndef mingw32_HOST_OS
  , pgDBAddr = Right (SockAddrUnix (fromMaybe "/tmp/.s.PGSQL.5432" (unsafePerformIO (lookupEnv "PGSOCK"))))
#endif
  , pgDBUser = "templatepg"
  -- , pgDBDebug = True
  , pgDBParams = [("TimeZone", "UTC")]
  }

