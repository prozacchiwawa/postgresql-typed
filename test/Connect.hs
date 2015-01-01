module Connect where

import Database.TemplatePG (PGDatabase(..), defaultPGDatabase)
import Network (PortID(UnixSocket))

db :: PGDatabase
db = defaultPGDatabase
  { pgDBPort = UnixSocket "/tmp/.s.PGSQL.5432"
  , pgDBName = "templatepg"
  , pgDBUser = "templatepg"
  }

