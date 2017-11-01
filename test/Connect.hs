{-# LANGUAGE OverloadedStrings #-}
module Connect where

import Database.PostgreSQL.Typed (PGDatabase(..), defaultPGDatabase)
import Network (PortID(UnixSocket))

db :: PGDatabase
db = defaultPGDatabase
  { pgDBName = "templatepg"
  , pgDBUser = "templatepg"
  , pgDBDebug = True
  , pgDBParams = [("TimeZone", "UTC")]
  }

