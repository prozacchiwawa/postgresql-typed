-- Parses postgresql/src/backend/utils/errcodes.txt into ErrCodes.hs
-- Based on generate-errcodes.pl
import Data.Char (isSpace, isLower, toLower)
import Data.List (intercalate, isPrefixOf, find, sortOn)
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Time.Clock (getCurrentTime)
import System.Directory (doesDirectoryExist)
import System.Environment (getArgs, getProgName)
import System.Exit (exitFailure)
import System.FilePath ((</>))
import System.IO (stderr, readFile, hPutStrLn)

path :: FilePath
path = "src" </> "backend" </> "utils" </> "errcodes.txt"

data ErrType
  = Error
  | Warning
  | Success
  deriving (Show)

data ErrCode = ErrCode
  { errCode :: String
  , errMacro :: String
  , errName :: Maybe String
  , errType :: ErrType
  }

data Line
  = Line ErrCode
  | Section String

macroName :: String -> String
macroName ('E':'R':'R':'C':'O':'D':'E':'_':n) = n
macroName n = n

descName :: ErrCode -> String
descName ErrCode{ errName = Just n } = n
descName ErrCode{ errMacro = n } = n

macroPrefixes :: [String]
macroPrefixes = ["WARNING_", "S_R_E_", "E_R_E_", "E_R_I_E_"]

varName :: ErrCode -> String
varName ErrCode{ errName = Just n@(h:_), errMacro = m }
  | Just p <- find (`isPrefixOf` m) macroPrefixes = map toLower p ++ n
  | isLower h = n
varName e = '_':descName e

parseType :: String -> Maybe ErrType
parseType "E" = Just Error
parseType "W" = Just Warning
parseType "S" = Just Success
parseType _ = Nothing

parseWords :: [String] -> Maybe ErrCode
parseWords [c@[_,_,_,_,_], t, m, n] = ErrCode c (macroName m) (Just n) <$> parseType t
parseWords [c@[_,_,_,_,_], t, m]    = ErrCode c (macroName m) Nothing  <$> parseType t
parseWords _ = Nothing

parseLine :: String -> Maybe Line
parseLine ('#':_) = Nothing
parseLine ('S':'e':'c':'t':'i':'o':'n':':':s) = Just $ Section $ dropWhile isSpace s
parseLine s
  | all isSpace s = Nothing
  | otherwise = Just $ Line $ fromMaybe (error $ "invalid line: " ++ s) $ parseWords $ words s

exportLine :: Line -> IO ()
exportLine (Section s) = putStrLn $ "  -- * " ++ s
exportLine (Line e) = putStrLn $ "  , " ++ varName e

lineErr :: Line -> Maybe ErrCode
lineErr (Line e) = Just e
lineErr _ = Nothing

line :: ErrCode -> IO ()
line e = do
  putStrLn $ ""
  putStrLn $ "-- |@" ++ errMacro e ++ "@: " ++ errCode e ++ " (" ++ show (errType e) ++ ")"
  putStrLn $ varName e ++ " :: ByteString"
  putStrLn $ varName e ++ " = " ++ show (errCode e)

name :: ErrCode -> Maybe String
name e = Just $ "(" ++ varName e ++ "," ++ show (descName e) ++ ")"

main :: IO ()
main = do
  prog <- getProgName
  args <- getArgs
  arg <- case args of
    [f] -> return f
    _ -> do
      hPutStrLn stderr $ "Usage: " ++ prog ++ " POSTGRESQLSRCDIR[/" ++ path ++ "] > ErrCodes.hs"
      exitFailure
  argd <- doesDirectoryExist arg
  let file | argd = arg </> path
           | otherwise = arg
  l <- mapMaybe parseLine . lines <$> readFile file
  let e = mapMaybe lineErr l
  now <- getCurrentTime
  putStrLn $ "-- Automatically generated from " ++ file ++ " using " ++ prog ++ " " ++ show now ++ "."
  putStrLn $ "{-# LANGUAGE OverloadedStrings #-}"
  putStrLn $ "-- |PostgreSQL error codes."
  putStrLn $ "module Database.PostgreSQL.Typed.ErrCodes (names"
  mapM_ exportLine l
  putStrLn $ ") where"
  putStrLn $ ""
  putStrLn $ "import Data.ByteString (ByteString)"
  putStrLn $ "import Data.Map.Strict (Map, fromDistinctAscList)"
  mapM_ line e
  putStrLn $ ""
  putStrLn $ "-- |All known error code names by code."
  putStrLn $ "names :: Map ByteString String"
  putStrLn $ "names = fromDistinctAscList\n  [" ++ intercalate "\n  ," (mapMaybe name $ sortOn errCode e) ++ "]"
  return ()
