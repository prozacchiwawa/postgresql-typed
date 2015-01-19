{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, FunctionalDependencies, DataKinds #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
-- |
-- Module: Database.PostgreSQL.Typed.Inet
-- Copyright: 2015 Dylan Simon
-- 
-- Representaion of PostgreSQL's inet/cidr types using "Network.Socket".
-- We don't (yet) supply PGColumn (parsing) instances.

module Database.PostgreSQL.Typed.Inet where

import qualified Data.ByteString.Char8 as BSC
import Data.Maybe (fromJust)
import qualified Network.Socket as Net
import System.IO.Unsafe (unsafeDupablePerformIO)

import Database.PostgreSQL.Typed.Types

data PGInet 
  = PGInet
    { pgInetAddr :: !Net.HostAddress
    , pgInetMask :: !Int
    }
  | PGInet6
    { pgInetAddr6 :: !Net.HostAddress6
    , pgInetMask :: !Int
    }

sockAddrPGInet :: Net.SockAddr -> Maybe PGInet
sockAddrPGInet (Net.SockAddrInet _ a) = Just $ PGInet a 32
sockAddrPGInet (Net.SockAddrInet6 _ _ a _) = Just $ PGInet6 a 128
sockAddrPGInet _ = Nothing

instance Show PGInet where
  -- This is how Network.Socket's Show SockAddr does it:
  show (PGInet a 32) = unsafeDupablePerformIO $ Net.inet_ntoa a
  show (PGInet a m) = show (PGInet a 32) ++ '/' : show m
  show (PGInet6 a 128) = fromJust $ fst $ unsafeDupablePerformIO $
    Net.getNameInfo [Net.NI_NUMERICHOST] True False (Net.SockAddrInet6 0 0 a 0)
  show (PGInet6 a m) = show (PGInet6 a 128) ++ '/' : show m

instance PGType "inet"
instance PGType "cidr"
instance PGParameter "inet" PGInet where
  pgEncode _ = BSC.pack . show
instance PGParameter "cidr" PGInet where
  pgEncode _ = BSC.pack . show
