{-# LANGUAGE PatternSynonyms, TemplateHaskell #-}
module Database.PostgreSQL.Typed.Internal
  ( stringE
  , pattern StringE
  , ($++$)
  , (++$)
  ) where

import Data.String (IsString(..))
import qualified Language.Haskell.TH as TH

stringE :: String -> TH.Exp
stringE = TH.LitE . TH.StringL

pattern StringE s = TH.LitE (TH.StringL s)
pattern InfixE l o r = TH.InfixE (Just l) (TH.VarE o) (Just r)

instance IsString TH.Exp where
  fromString = stringE

($++$) :: TH.Exp -> TH.Exp -> TH.Exp
infixr 5 $++$
StringE s $++$ r = s ++$ r
l $++$ StringE "" = l
InfixE ll pp (StringE lr) $++$ StringE r | pp == '(++) = ll $++$ StringE (lr ++ r)
l $++$ r = InfixE l '(++) r

(++$) :: String -> TH.Exp -> TH.Exp
infixr 5 ++$
"" ++$ r = r
l ++$ StringE r = StringE (l ++ r)
l ++$ InfixE (StringE rl) pp rr | pp == '(++) = (l ++ rl) ++$ rr
l ++$ r = InfixE (StringE l) '(++) r
