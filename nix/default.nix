{ pkgsPath ? null
, compiler ? "ghc864"
, postgresql ? "postgresql"
}:
let
  # We pin the nixpkgs version here to ensure build reproducibility
  pinnedNixpkgs =
    import ./fetch-nixpkgs.nix
      { # Latest HEAD of the release-19.03 branch as of 2019-05-22
        rev = "23a3bda4da71f6f6a7a248c593e14c838b75d40b";
        # This sha256 can be obtained with:
        # `$ nix-prefetch-url https://github.com/NixOS/nixpkgs/archive/${rev}.tar.gz`
        sha256 = "0v2r8xr3nvpc5xfqr4lr6i3mrcn6d5np1dr26q4iks5hj2zlxl97";
        # This one with:
        # `$ nix-prefetch-url --unpack https://github.com/NixOS/nixpkgs/archive/${rev}.tar.gz`
        outputSha256 = "1sgi9zi4h3mhvjq2wkzkvq2zdvh6p9qg19yv7c01sqin23s6yqr1";
      };

  # Use pkgsPath if provided, else the pinned checkout
  realPkgsPath =
    if pkgsPath == null then pinnedNixpkgs else pkgsPath;

  # This overlay extends the nixpkgs' package set a custom haskell package set
  # which includes postgresql-typed
  overlay = self: super:
    {
      myHaskellPackages = self.haskell.packages."${compiler}".override (_ : {
        overrides = haskellOverlay self;
        });
    };

  # This overlay extends a haskell package set with postgresql-typed
  haskellOverlay = pkgs: self: super: with pkgs.haskell.lib;
    {
      # version without TLS
      postgresql-typed =
        let
          src = pkgs.lib.cleanSource ../.;
          drv = self.callCabal2nix "postgresql-typed" src {};
          drvWithPostgres = withPostgres pkgs.${postgresql} drv;
        in appendConfigureFlag drvWithPostgres "-f-TLS";

      # version with TLS
      postgresql-typed-tls = overrideCabal self.postgresql-typed (old: {
        configureFlags = old.configureFlags or [] ++ ["-fTLS"];
        #checkPhase = "..."; #TODO
      });
    };
in import realPkgsPath
  { overlays =
    [ overlay
      (import ./utilities.nix)
    ];
  }
