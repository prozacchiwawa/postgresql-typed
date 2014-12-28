module Database.TemplatePG.Query
  ( PGQuery
  , pgExecute
  , pgQuery
  , makePGQuery
  ) where

import Control.Applicative ((<$>))
import Control.Monad (zipWithM, liftM)
import Data.Maybe (fromJust)
import Language.Haskell.Meta.Parse (parseExp)
import qualified Language.Haskell.TH as TH
import qualified Text.ParserCombinators.Parsec as P

import Database.TemplatePG.Types
import Database.TemplatePG.Protocol
import Database.TemplatePG.Connection

-- |A query returning rows of the given type.
data PGQuery a = PGSimpleQuery 
  { pgQueryString :: String
  , pgQueryParser :: PGData -> a
  }

instance Functor PGQuery where
  fmap f q = q{ pgQueryParser = f . pgQueryParser q }

-- |Run a query and return a list of row results.
pgQuery :: PGConnection -> PGQuery a -> IO [a]
pgQuery c PGSimpleQuery{ pgQueryString = s, pgQueryParser = p } =
  map p . snd <$> pgSimpleQuery s c

-- |Execute a query that does not return result.
-- Return the number of rows affected (or -1 if not known).
pgExecute :: PGConnection -> PGQuery () -> IO Int
pgExecute c PGSimpleQuery{ pgQueryString = s } =
  fst <$> pgSimpleQuery s c

-- |Produce a new PGQuery from a SQL query string.
-- This should be used as @$(makePGQuery \"SELECT ...\")@
makePGQuery :: String -- ^ a SQL query string
            -> TH.Q TH.Exp -- ^ a PGQuery
makePGQuery sql = do
  (pTypes, fTypes) <- TH.runIO $ withTHConnection $ \c ->
    describeStatement c (holdPlaces sqlStrings expStrings)
  s <- weaveString sqlStrings =<< zipWithM stringify pTypes expStrings
  [| PGSimpleQuery $(return s) $(convertRow fTypes) |]
  where
  holdPlaces ss es = concat $ weave ss (take (length es) placeholders)
  placeholders = map (('$' :) . show) ([1..]::[Int])
  stringify t s = [| $(pgTypeEscaper t) $(parseExp' s) |]
  parseExp' e = either (fail . (++) ("Failed to parse expression {" ++ e ++ "}: ")) return $ parseExp e
  (sqlStrings, expStrings) = parseSql sql

-- |"weave" 2 lists of equal length into a single list.
weave :: [a] -> [a] -> [a]
weave x []          = x
weave [] y          = y
weave (x:xs) (y:ys) = x:y:(weave xs ys)

-- |"weave" a list of SQL fragements an Haskell expressions into a single SQL string.
weaveString :: [String] -- ^ SQL fragments
            -> [TH.Exp]    -- ^ Haskell expressions
            -> TH.Q TH.Exp
weaveString []     []     = [| "" |]
weaveString [x]    []     = [| x |]
weaveString []     [y]    = return y
weaveString (x:xs) (y:ys) = [| x ++ $(return y) ++ $(weaveString xs ys) |]
weaveString _      _      = error "Weave mismatch (possible parse problem)"

-- |Given a result description, create a function to convert a result to a
-- tuple.
convertRow :: [(String, PGTypeHandler, Bool)] -- ^ result description
           -> TH.Q TH.Exp -- ^ A function for converting a row of the given result description
convertRow types = do
  n <- TH.newName "result"
  TH.lamE [TH.varP n] $ TH.tupE $ map (convertColumn n) $ zip types [0..]

-- |Given a raw PostgreSQL result and a result field type, convert the
-- appropriate field to a Haskell value.
convertColumn :: TH.Name  -- ^ the name of the variable containing the result list (of 'Maybe' 'ByteString')
              -> ((String, PGTypeHandler, Bool), Int) -- ^ the result field type and index
              -> TH.Q TH.Exp
convertColumn name ((_, typ, nullable), i) = [| $(pgStringToType' typ nullable) ($(TH.varE name) !! i) |]

-- SQL Parser --

every2nd :: [a] -> ([a], [a])
every2nd = foldr (\a ~(x,y) -> (a:y,x)) ([],[])

-- |Given a SQL string return a list of SQL parts and expression parts.
-- For example: @\"SELECT * FROM table WHERE id = {someID} AND age > {baseAge * 1.5}\"@
-- becomes: @(["SELECT * FROM table WHERE id = ", " AND age > "],
--            ["someID", "baseAge * 1.5"])@
parseSql :: String -> ([String], [String])
parseSql sql = case (P.parse sqlStatement "" sql) of
                 Left err -> error (show err)
                 Right ss -> every2nd ss

-- |Like 'pgStringToType', but deal with possible @NULL@s. If the boolean
-- argument is 'False', that means that we know that the value is not nullable
-- and we can use 'fromJust' to keep the code simple. If it's 'True', then we
-- don't know if the value is nullable and must return a 'Maybe' value in case
-- it is.
pgStringToType' :: PGTypeHandler
                -> Bool  -- ^ nullability indicator
                -> TH.Q TH.Exp
pgStringToType' t False = [| $(pgTypeDecoder t) . fromJust |]
pgStringToType' t True  = [| liftM $(pgTypeDecoder t) |]

sqlStatement :: P.Parser [String]
sqlStatement = P.many1 $ P.choice [sqlText, sqlParameter]

sqlText :: P.Parser String
sqlText = P.many1 (P.noneOf "{")

-- |Parameters are enclosed in @{}@ and can be any Haskell expression supported
-- by haskell-src-meta.
sqlParameter :: P.Parser String
sqlParameter = P.between (P.char '{') (P.char '}') $ P.many1 (P.noneOf "}")
