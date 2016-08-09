{-# LANGUAGE CPP, MultiParamTypeClasses, FlexibleInstances, UndecidableInstances, FunctionalDependencies, DataKinds, GeneralizedNewtypeDeriving, PatternGuards, OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
-- |
-- Module: Database.PostgreSQL.Typed.Range
-- Copyright: 2015 Dylan Simon
-- 
-- Representaion of PostgreSQL's range type.
-- There are a number of existing range data types, but PostgreSQL's is rather particular.
-- This tries to provide a one-to-one mapping.

module Database.PostgreSQL.Typed.Range where

#if !MIN_VERSION_base(4,8,0)
import Control.Applicative ((<$>), (<$))
#endif
import Control.Monad (guard)
import qualified Data.Attoparsec.ByteString.Char8 as P
import qualified Data.ByteString.Builder as BSB
import qualified Data.ByteString.Char8 as BSC
import Data.Monoid ((<>))
#if !MIN_VERSION_base(4,8,0)
import Data.Monoid (Monoid(..))
#endif

import Database.PostgreSQL.Typed.Types

-- |A end-point for a range, which may be nothing (infinity, NULL in PostgreSQL), open (inclusive), or closed (exclusive)
data Bound a
  = Unbounded -- ^ Equivalent to @Bounded False ±Infinity@
  | Bounded
    { _boundClosed :: Bool -- ^ @True@ if the range includes this bound
    , _bound :: a
    }
  deriving (Eq)

instance Functor Bound where
  fmap _ Unbounded = Unbounded
  fmap f (Bounded c a) = Bounded c (f a)

newtype LowerBound a = Lower { boundLower :: Bound a } deriving (Eq, Functor)

-- |Takes into account open vs. closed (but does not understand equivalent discrete bounds)
instance Ord a => Ord (LowerBound a) where
  compare (Lower Unbounded) (Lower Unbounded) = EQ
  compare (Lower Unbounded) _ = LT
  compare _ (Lower Unbounded) = GT
  compare (Lower (Bounded ac a)) (Lower (Bounded bc b)) = compare a b <> compare bc ac

-- |The constraint is only necessary for @maxBound@, unfortunately
instance Bounded a => Bounded (LowerBound a) where
  minBound = Lower Unbounded
  maxBound = Lower (Bounded False maxBound)

newtype UpperBound a = Upper { boundUpper :: Bound a } deriving (Eq, Functor)

-- |Takes into account open vs. closed (but does not understand equivalent discrete bounds)
instance Ord a => Ord (UpperBound a) where
  compare (Upper Unbounded) (Upper Unbounded) = EQ
  compare (Upper Unbounded) _ = GT
  compare _ (Upper Unbounded) = LT
  compare (Upper (Bounded ac a)) (Upper (Bounded bc b)) = compare a b <> compare ac bc

-- |The constraint is only necessary for @minBound@, unfortunately
instance Bounded a => Bounded (UpperBound a) where
  minBound = Upper (Bounded False minBound)
  maxBound = Upper Unbounded

compareBounds :: Ord a => LowerBound a -> UpperBound a -> Bound Bool
compareBounds (Lower (Bounded lc l)) (Upper (Bounded uc u)) =
  case compare l u of
    LT -> Bounded True True
    EQ -> Bounded (lc /= uc) (lc && uc)
    GT -> Bounded False False
compareBounds _ _ = Unbounded

data Range a
  = Empty
  | Range
    { lower :: LowerBound a
    , upper :: UpperBound a
    }
  deriving (Eq, Ord)

instance Functor Range where
  fmap _ Empty = Empty
  fmap f (Range l u) = Range (fmap f l) (fmap f u)

instance Show a => Show (Range a) where
  showsPrec _ Empty = showString "empty"
  showsPrec _ (Range (Lower l) (Upper u)) =
    sc '[' '(' l . sb l . showChar ',' . sb u . sc ']' ')' u where
    sc c o b = showChar $ if boundClosed b then c else o
    sb = maybe id (showsPrec 10) . bound

bound :: Bound a -> Maybe a
bound Unbounded = Nothing
bound (Bounded _ b) = Just b

-- |Unbounded endpoints are always open.
boundClosed :: Bound a -> Bool
boundClosed Unbounded = False
boundClosed (Bounded c _) = c

-- |Construct from parts: @makeBound (boundClosed b) (bound b) == b@
makeBound :: Bool -> Maybe a -> Bound a
makeBound c (Just a) = Bounded c a
makeBound False Nothing = Unbounded
makeBound True Nothing = error "makeBound: unbounded may not be closed"

-- |Empty ranges treated as 'Unbounded'
lowerBound :: Range a -> Bound a
lowerBound Empty = Unbounded
lowerBound (Range (Lower b) _) = b

-- |Empty ranges treated as 'Unbounded'
upperBound :: Range a -> Bound a
upperBound Empty = Unbounded
upperBound (Range _ (Upper b)) = b

-- |Equivalent to @boundClosed . lowerBound@
lowerClosed :: Range a -> Bool
lowerClosed Empty = False
lowerClosed (Range (Lower b) _) = boundClosed b

-- |Equivalent to @boundClosed . upperBound@
upperClosed :: Range a -> Bool
upperClosed Empty = False
upperClosed (Range _ (Upper b)) = boundClosed b

empty :: Range a
empty = Empty

isEmpty :: Ord a => Range a -> Bool
isEmpty Empty = True
isEmpty (Range l u)
  | Bounded _ n <- compareBounds l u = not n
  | otherwise = False

full :: Range a
full = Range (Lower Unbounded) (Upper Unbounded)

isFull :: Range a -> Bool
isFull (Range (Lower Unbounded) (Upper Unbounded)) = True
isFull _ = False

-- |Create a point range @[x,x]@
point :: a -> Range a
point a = Range (Lower (Bounded True a)) (Upper (Bounded True a))

-- |Extract a point: @getPoint (point x) == Just x@
getPoint :: Eq a => Range a -> Maybe a
getPoint (Range (Lower (Bounded True l)) (Upper (Bounded True u))) = u <$ guard (u == l)
getPoint _ = Nothing

-- Construct a range from endpoints and normalize it.
range :: Ord a => Bound a -> Bound a -> Range a
range l u = normalize $ Range (Lower l) (Upper u)

-- Construct a standard range (@[l,u)@ or 'point') from bounds (like 'bound') and normalize it.
normal :: Ord a => Maybe a -> Maybe a -> Range a
normal l u = range (mb True l) (mb (l == u) u) where
  mb = maybe Unbounded . Bounded

-- Construct a bounded range like 'normal'.
bounded :: Ord a => a -> a -> Range a
bounded l u = normal (Just l) (Just u)

-- Fold empty ranges to 'Empty'.
normalize :: Ord a => Range a -> Range a
normalize r
  | isEmpty r = Empty
  | otherwise = r

-- |'normalize' for discrete (non-continuous) range types, using the 'Enum' instance
normalize' :: (Ord a, Enum a) => Range a -> Range a
normalize' Empty = Empty
normalize' (Range (Lower l) (Upper u)) = normalize $ range l' u'
  where
  l' = case l of
    Bounded False b -> Bounded True (succ b)
    _ -> l
  u' = case u of
    Bounded True b -> Bounded False (succ b)
    _ -> u

-- |Contains range
(@>), (<@) :: Ord a => Range a -> Range a -> Bool
_ @> Empty = True
Empty @> r = isEmpty r
Range la ua @> Range lb ub = la <= lb && ua >= ub
a <@ b = b @> a

-- |Contains element
(@>.) :: Ord a => Range a -> a -> Bool
r @>. a = r @> point a

overlaps :: Ord a => Range a -> Range a -> Bool
overlaps a b = intersect a b /= Empty

intersect :: Ord a => Range a -> Range a -> Range a
intersect (Range la ua) (Range lb ub) = normalize $ Range (max la lb) (min ua ub)
intersect _ _ = Empty

instance Ord a => Monoid (Range a) where
  mempty = Empty
  -- |Union ranges.  Fails if ranges are disjoint.
  mappend Empty r = r
  mappend r Empty = r
  mappend _ra@(Range la ua) _rb@(Range lb ub)
    -- isEmpty _ra = _rb
    -- isEmpty _rb = _ra
    | Bounded False False <- compareBounds lb ua = error "mappend: disjoint Ranges"
    | Bounded False False <- compareBounds la ub = error "mappend: disjoint Ranges"
    | otherwise = Range (min la lb) (max ua ub)


-- |Class indicating that the first PostgreSQL type is a range of the second.
-- This implies 'PGParameter' and 'PGColumn' instances that will work for any type.
class (PGType tr, PGType t) => PGRangeType tr t | tr -> t where
  pgRangeElementType :: PGTypeName tr -> PGTypeName t
  pgRangeElementType PGTypeProxy = PGTypeProxy

instance (PGRangeType tr t, PGParameter t a) => PGParameter tr (Range a) where
  pgEncode _ Empty = BSC.pack "empty"
  pgEncode tr (Range (Lower l) (Upper u)) = buildPGValue $
    pc '[' '(' l
      <> pb (bound l)
      <> BSB.char7 ','
      <> pb (bound u)
      <> pc ']' ')' u
    where
    pb Nothing = mempty
    pb (Just b) = pgDQuote "(),[]" $ pgEncode (pgRangeElementType tr) b
    pc c o b = BSB.char7 $ if boundClosed b then c else o
instance (PGRangeType tr t, PGColumn t a) => PGColumn tr (Range a) where
  pgDecode tr a = either (error . ("pgDecode range (" ++) . (++ ("): " ++ BSC.unpack a))) id $ P.parseOnly per a where
    per = (Empty <$ pe) <> pr
    pe = P.stringCI "empty"
    pb = fmap (pgDecode (pgRangeElementType tr)) <$> parsePGDQuote True "(),[]" BSC.null
    pc c o = (True <$ P.char c) <> (False <$ P.char o)
    mb = maybe Unbounded . Bounded
    pr = do
      lc <- pc '[' '('
      lb <- pb
      _ <- P.char ','
      ub <- pb 
      uc <- pc ']' ')'
      return $ Range (Lower (mb lc lb)) (Upper (mb uc ub))

instance PGType "int4range"
instance PGRangeType "int4range" "integer"
instance PGType "numrange"
instance PGRangeType "numrange" "numeric"
instance PGType "tsrange"
instance PGRangeType "tsrange" "timestamp without time zone"
instance PGType "tstzrange"
instance PGRangeType "tstzrange" "timestamp with time zone"
instance PGType "daterange"
instance PGRangeType "daterange" "date"
instance PGType "int8range"
instance PGRangeType "int8range" "bigint"

