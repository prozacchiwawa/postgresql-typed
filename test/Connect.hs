{-# LANGUAGE OverloadedStrings #-}
module Connect where

import Database.PostgreSQL.Typed (PGDatabase(..), defaultPGDatabase)
import Network (PortID(UnixSocket))

db :: PGDatabase
db = defaultPGDatabase
  { pgDBPort = UnixSocket "/tmp/.s.PGSQL.5432"
  , pgDBName = "templatepg"
  , pgDBUser = "templatepg"
  , pgDBDebug = True
  }

