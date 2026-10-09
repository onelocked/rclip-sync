{
  inputs.nixpkgs.url = "https://channels.nixos.org/nixpkgs-unstable/nixexprs.tar.zst";
  outputs =
    { nixpkgs, ... }:
    let
      inherit (nixpkgs) lib;
      forEachPkgs = f: lib.genAttrs [ "x86_64-linux" ] (system: f nixpkgs.legacyPackages.${system});

      rclip-sync =
        {
          lib,
          stdenv,
          rustPlatform,
          windows,
        }:

        rustPlatform.buildRustPackage {
          pname = "rclip-sync";
          version = "1.0";

          src = ./.;
          cargoLock.lockFile = ./Cargo.lock;

          buildInputs = lib.optional stdenv.hostPlatform.isWindows windows.pthreads;

          doCheck = false;
        };
    in
    {
      packages = forEachPkgs (pkgs: {
        rclip-sync = pkgs.callPackage rclip-sync { };
        rclip-sync-windows = pkgs.pkgsCross.mingwW64.callPackage rclip-sync { };
      });
    };
}
