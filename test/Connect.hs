{-# LANGUAGE CPP, OverloadedStrings #-}
module Connect where

import Database.PostgreSQL.Typed (PGDatabase(..), defaultPGDatabase)
import Network.Socket (SockAddr(SockAddrUnix))

db :: PGDatabase
db = defaultPGDatabase
  { pgDBName = "templatepg"
#ifndef mingw32_HOST_OS
  , pgDBAddr = Right (SockAddrUnix "/tmp/.s.PGSQL.5432")
#endif
  , pgDBUser = "templatepg"
  -- , pgDBDebug = True
  , pgDBParams = [("TimeZone", "UTC")]
  }

