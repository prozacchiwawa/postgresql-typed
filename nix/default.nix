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
      # version with TLS
      postgresql-typed =
        let
          src = pkgs.lib.cleanSource ../.;
          drv = self.callCabal2nix "postgresql-typed" src {};
          drvWithPostgres = withPostgres pkgs.${postgresql} drv;
        in pkgs.lib.overrideDerivation drvWithPostgres (old: {
          checkPhase = ''
            ${pkgs.openssl}/bin/openssl req -x509 -newkey rsa:2048 \
              -keyout $PGDATA/server.key \
              -out $PGDATA/server.crt \
              -days 1 -nodes \
              -subj "/C=US/ST=Somewhere/L=Earth/O=Test Network/OU=IT Department/CN=localhost"
            chmod 0600 $PGDATA/server.key
            echo 'ssl = on' >> $PGDATA/postgresql.conf
            # avoid conflicts on travis and elsewhere
            echo 'port = 5433' >> $PGDATA/postgresql.conf

            # disallow non-ssl connections to make sure we're doing TLS
            echo 'hostssl templatepg templatepg all trust' > $PGDATA/pg_hba.conf
            echo 'hostnossl all all all reject' >> $PGDATA/pg_hba.conf

            pg_ctl restart

            export PGTLS=1
            export PGPORT=5433

            # First test TlsNoValidate
            ./Setup test

            # Test TlsValidateCA
            export PGTLS_ROOTCERT=$(cat $PGDATA/server.crt)
            ./Setup test

            # Test TlsValidateFull
            export PGTLS_VALIDATEFULL=1
            ./Setup test

            # Test that cert validation fails with invalid cert
            ${pkgs.openssl}/bin/openssl req -x509 -newkey rsa:2048 \
              -keyout other.key \
              -out other.crt \
              -days 1 -nodes \
              -subj "/C=US/ST=Somewhere/L=Earth/O=Test Network/OU=IT Department/CN=localhost"
            export PGTLS_ROOTCERT=$(cat other.crt)
            ./Setup test && false || true
            '';
          });

      # version without TLS
      postgresql-typed-notls = pkgs.lib.overrideDerivation self.postgresql-typed (old: {
        configureFlags = old.configureFlags or [] ++ ["-f-TLS"];
        checkPhase = "./Setup test";
      });
    };
in import realPkgsPath
  { overlays =
    [ overlay
      (import ./utilities.nix)
    ];
  }
