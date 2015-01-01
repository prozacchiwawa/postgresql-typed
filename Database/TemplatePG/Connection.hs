module Database.TemplatePG.Connection 
  ( withTHConnection
  , useTHConnection
  , registerPGType
  ) where

import Control.Applicative ((<$>), (<$))
import Control.Concurrent.MVar (MVar, newMVar, modifyMVar, modifyMVar_, swapMVar)
import Control.Monad ((>=>), when, void)
import Data.Maybe (fromMaybe)
import qualified Language.Haskell.TH as TH
import Network (PortID(UnixSocket, PortNumber), PortNumber)
import System.Environment (getEnv, lookupEnv)
import System.IO.Unsafe (unsafePerformIO)

import Database.TemplatePG.Types
import Database.TemplatePG.Protocol

-- |Grab a PostgreSQL connection for compile time. We do so through the
-- environment variables: @TPG_DB@, @TPG_HOST@, @TPG_PORT@, @TPG_USER@, and
-- @TPG_PASS@. Only TPG_DB is required.
thConnection :: MVar (Either (IO PGConnection) PGConnection)
thConnection = unsafePerformIO $ newMVar $ Left $ do
  database <- getEnv "TPG_DB"
  hostName <- fromMaybe "localhost" <$> lookupEnv "TPG_HOST"
  socket   <- lookupEnv "TPG_SOCK"
  portNum  <- maybe (5432 :: PortNumber) ((fromIntegral :: Int -> PortNumber) . read) <$> lookupEnv "TPG_PORT"
  username <- fromMaybe "postgres" <$> lookupEnv "TPG_USER"
  password <- fromMaybe "" <$> lookupEnv "TPG_PASS"
  let portId = maybe (PortNumber portNum) UnixSocket socket
  pgConnect hostName portId database username password

-- |Run an action using the TemplatePG connection.
-- This is meant to be used from other TH code (though it will work during normal runtime if just want a simple PGConnection based on TPG environment variables).
withTHConnection :: (PGConnection -> IO a) -> IO a
withTHConnection f = modifyMVar thConnection $ either id return >=> (\c -> (,) (Right c) <$> f c)

setTHConnection :: Either (IO PGConnection) PGConnection -> IO ()
setTHConnection = void . swapMVar thConnection

-- |Specify an alternative connection method to use during TemplatePG compilation.
-- This lets you override the default connection parameters that are based on TPG environment variables.
-- This should be called as a top-level declaration and produces no code.
useTHConnection :: IO PGConnection -> TH.Q [TH.Dec]
useTHConnection c = [] <$ TH.runIO (setTHConnection (Left c))

modifyTHConnection :: (PGConnection -> PGConnection) -> IO ()
modifyTHConnection f = modifyMVar_ thConnection $ return . either (Left . fmap f) (Right . f)

-- |Register a new handler for PostgreSQL type and a Haskell type, which should be an instance of 'PGType'.
-- This should be called as a top-level declaration and produces no code.
registerPGType :: String -> TH.Type -> TH.Q [TH.Dec]
registerPGType name typ = [] <$ TH.runIO (do
  (oid, loid) <- withTHConnection (\c -> getTypeOID c name)
  modifyTHConnection (pgAddType oid (PGType name typ))
  when (loid /= 0) $
    modifyTHConnection (pgAddType loid (pgArrayType name typ)))
