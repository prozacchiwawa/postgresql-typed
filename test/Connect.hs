module Connect where

import Database.TemplatePG (pgConnect)
import Network (PortID(UnixSocket))

connect = pgConnect "localhost" (UnixSocket "/tmp/.s.PGSQL.5432") "templatepg" "templatepg" ""
