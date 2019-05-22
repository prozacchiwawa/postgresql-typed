{ pkgsPath ? null
, compiler ? "ghc864"
, postgresql ? "postgresql"
}@args:
let pkgs = import ./. args;
in
{
  inherit (pkgs.myHaskellPackages)
    postgresql-typed
    postgresql-typed-notls;
}
