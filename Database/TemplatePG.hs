-- Copyright 2010, 2011, 2012, 2013 Chris Forno
-- Copyright 2014 Dylan Simon

module Database.TemplatePG 
  (
  -- *Introduction
  -- $intro

    PGError(..)

  -- *Usage
  -- $usage

  -- **Connections
  -- $connect

  , PGConnection
  , pgConnect
  , pgDisconnect
  , useTHConnection

  -- **Queries
  -- $query
  
  -- ***Compile time
  -- $compile
  , makePGSimpleQuery
  , makePGPreparedQuery

  -- ***Runtime
  -- $run
  , pgQuery
  , pgExecute

  -- **Basic queries
  -- $basic

  , queryTuples
  , queryTuple
  , execute
  -- *Advanced usage

  -- **Types
  -- $types

  , registerPGType

  -- **A Note About NULL
  -- $nulls

  -- *Caveats
  -- $caveats

  , withTransaction
  , rollback
  , insertIgnore

  -- **Tips
  -- $tips

  ) where

import Database.TemplatePG.Protocol
import Database.TemplatePG.Connection
import Database.TemplatePG.Query
import Database.TemplatePG.SQL

-- $intro
-- TemplatePG is designed with 2 goals in mind: safety and performance. The
-- primary focus is on safety.
--
-- To help ensure safety, it uses the PostgreSQL server to parse every query
-- and statement in your code to infer types at compile-time. This means that
-- in theory you cannot get a syntax error at runtime. Getting proper types at
-- compile time has the nice side-effect that it eliminates run-time type
-- casting and usually results in less code. This approach was inspired by
-- MetaHDBC (<http://haskell.org/haskellwiki/MetaHDBC>) and PG'OCaml
-- (<http://pgocaml.berlios.de/>).
--
-- While compile-time query analysis eliminates many errors, it doesn't
-- eliminate all of them. If you modify the database without recompilation or
-- have an error in a trigger or function, for example, you can still trigger a
-- 'PGException' or other failure (if types change).  Also, nullable result fields resulting from outer joins are not
-- detected and need to be handled specially.
--
-- Use the software at your own risk. Note however that TemplatePG is currently powering
-- <http://www.vocabulink.com/> with no problems yet. (For usage examples, you
-- can see the Vocabulink source code at <https://github.com/jekor/vocabulink>).

-- $usage
-- Basic usage consists of calling 'pgConnect', 'makePGSimpleQuery' (Template Haskell), 'pgQuery', and 'pgDisconnect':
--
-- @
-- c <- pgConnect
-- let name = \"Joe\"
-- people :: [Int32] <- pgQuery c $(makePGSimpleQuery "SELECT id FROM people WHERE name = ${name}")
-- pgDisconnect c
-- @

-- $connect
-- All database access requires a 'PGConnection' that is created at runtime using 'pgConnect', and should be explicitly be closed with 'pgDisconnect' when finished.
-- 
-- However, at compile time, TemplatePG needs to make its own connection to the database in order to describe queries.
-- By default, it will use the following environment variables. You must set at least @TPG_DB@:
-- 
-- [@TPG_DB@] the database name to use
-- 
-- [@TPG_USER@] the username to connect as (default: @postgres@)
-- 
-- [@TPG_PASS@] the password to use (default: /empty/)
-- 
-- [@TPG_HOST@] the host to connect to (default: @localhost@)
-- 
-- [@TPG_PORT@ or @TPG_SOCK@] the port number or local socket path to connect on (default: @5432@)
-- 
-- If you'd like to specify what connection to use directly, use 'useTHConnection' at the top level:
--
-- @
-- myConnect = pgConnect ...
-- useTHConnection myConnect
-- @
--
-- Note that due to TH limitations, @myConnect@ must be in-line or in a different module, and must be processed by the compiler before (above) any other TH calls.
--
-- You can set @TPG_DEBUG@ at compile or runtime to get a protocol-level trace.

-- $query
-- There are two steps to running a query: a Template Haskell function to perform type-inference at compile time and create a 'PGQuery' ('makePGSimpleQuery', 'makePGPreparedQuery'); and a run-time function to execute the query ('pgRunQuery', 'pgQuery', 'pgExecute').

-- $compile
-- Both TH functions take a single SQL string, which may contain in-line placeholders of the form @${expr}@ (where @expr@ is any valid Haskell expression that does not contain @{}@) and/or PostgreSQL placeholders of the form @$1@, @$2@, etc.
--
-- @let q = $(makePGSimpleQuery \"SELECT id, name, address FROM people WHERE name LIKE ${query++\\\"%\\\"} OR email LIKE $1") :: PGSimpleQuery [(Int32, String, Maybe String)]@
--
-- Expression placeholders are substituted by PostgreSQL ones in left-to-right order starting with 1, so must be in places that PostgreSQL allows them (e.g., not identifiers, table names, column names, operators, etc.)
-- However, this does mean that you can repeat expressions using the corresponding PostgreSQL placeholder as above.
-- If there are extra PostgreSQL parameters the may be passed as arguments:
--
-- @$(makePGSimpleQuery \"SELECT id FROM people WHERE name = $1\") :: String -> PGSimpleQuery [Int32]@
--
-- 'makePGPreparedQuery' works identically, but produces 'PGPreparedQuery' objects instead.
-- You can also create queries at run-time using 'rawPGSimpleQuery' or 'rawPGPreparedQuery'.

-- $run
-- There are multiple ways to run a 'PGQuery' once it's created ('pgQuery', 'pgExecute'), and you can also write your own, but they all reduce to 'pgRunQuery'.
-- These all take a 'PGConnection' and a 'PGQuery', and return results.
-- How they work depends on the type of query.
--
-- 'PGSimpleQuery' simply substitutes the placeholder values literally into into the SQL statement.  This should be safe for all currently-supported types.
-- 
-- 'PGPreparedQuery' is a bit more complex: the first time any given prepared query is run on a given connection, the query is prepared.  Every subsequent time, the previously-prepared query is re-used and the new placeholder values are bound to it.
-- Queries are identified by the text of the SQL statement with PostgreSQL placeholders in-place, so the exact parameter values do not matter (but the exact SQL statement, whitespace, etc. does).
-- (Prepared queries are released automatically at 'pgDisconnect', but may be closed early using 'pgCloseQuery'.)

-- $basic
-- There is also an older, simpler interface that combines both the compile and runtime steps.
-- 'queryTuples' does all the work ('queryTuple' and 'execute' are convenience
-- functions).
--
-- It's a Template Haskell function, so you need to splice it into your program
-- with @$()@. It requires a 'PGConnection' to a PostgreSQL server, but can't be
-- given one at compile-time, so you need to pass it after the splice:
--
-- @h <- pgConnect ...
-- 
-- tuples <- $(queryTuples \"SELECT * FROM pg_database\") h
-- @
--
-- To pass parameters to a query, include them in the string with {}. Most
-- Haskell expressions should work. For example:
--
-- @let owner = 33 :: Int32
-- 
-- tuples <- $(queryTuples \"SELECT * FROM pg_database WHERE datdba = {owner} LIMIT {2 * 3 :: Int32}\") h
-- @

-- $types
-- All supported types have instances of the 'PGType' class.
-- For the most part, only exactly equivalent types are used (e.g., 'Int32' for int4).
-- However, you can add support for your own types or replace the existing types just by making a new instance of 'PGType' and calling 'registerPGType' at the top level:
--
-- @
-- instance PGType MyType where ...
-- registerPGType \"mytype\" ''MyType
-- @
--
-- This will cause the PostgreSQL type @mytype@ to be converted to/from @MyType@.
-- Only one 'PGType' may be registered per PostgreSQL type, but the same 'PGType' may serve multiple PostgreSQL types.
-- This also automatically registers a handler for @_mytype@ (the PostgreSQL name for a vector or array of @mytype@) to @[Maybe MyType]@.
-- Like 'useTHConnection', this must be evaluated before any use of the type.

-- $nulls
-- Sometimes TemplatePG cannot determine whether or not a result field can
-- potentially be @NULL@. In those cases it will assume that it can. Basically,
-- any time a result field is not immediately tracable to an originating table
-- and column (such as when a function is applied to a result column), it's
-- assumed to be nullable and will be returned as a 'Maybe' value.
--
-- You can use @NULL@ values in parameters as well by using 'Maybe'.
--
-- Nullability is indicated incorrectly in the case of outer joins. TemplatePG
-- incorrectly infers that a field cannot be @NULL@ when it's able to trace the
-- result field back to a non-@NULL@ table column. As a workround, you can wrap
-- columns with @COALESCE()@ to force them to be returned as 'Maybe' values.
--
-- Because TemplatePG has to prepare statements at compile time and
-- placeholders can't be used in place of lists in PostgreSQL (such as @IN
-- (?)@), it's not currently possible to use non-static @IN ()@ clauses.

-- $caveats
-- I've included 'withTransaction', 'rollback', and 'insertIgnore', but they've
-- not been thoroughly tested, so use them at your own risk.
--
-- The types of any parameter expressions must be fully known.  This may
-- require explicit casts in some cases.
--
-- And in general, you cannot construct queries at run-time, since they
-- wouldn't be available to be analyzed at compile time.

-- $tips
-- If you find yourself pattern matching on result tuples just to pass them on
-- to functions, you can use @uncurryN@ from the tuple package. The following
-- examples are equivalent.
--
-- @(a, b, c) <- $(queryTuple \"SELECT a, b, c FROM {tableName} LIMIT 1\")
--
-- someFunction a b c
-- @
--
-- @uncurryN someFunction \`liftM\` $(queryTuple \"SELECT a, b, c FROM {tableName} LIMIT 1\")
-- @
