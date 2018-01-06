{-# LANGUAGE CPP, OverloadedStrings #-}
module Connect where

import Database.PostgreSQL.Typed (PGDatabase(..), defaultPGDatabase)
import Network (PortID(UnixSocket))

db :: PGDatabase
db = defaultPGDatabase
  { pgDBName = "templatepg"
#ifndef mingw32_HOST_OS
  , pgDBPort = UnixSocket "/tmp/.s.PGSQL.5432"
#endif
  , pgDBUser = "templatepg"
  -- , pgDBDebug = True
  , pgDBParams = [("TimeZone", "UTC")]
  }

