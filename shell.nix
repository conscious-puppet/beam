{ nixpkgs ? import ./nix/nixpkgs.nix { }
, ghc ? nixpkgs.haskellPackages
}:
with nixpkgs;

let
  beamLib = import ./nix/lib.nix { inherit nixpkgs; };
  beamGhc = beamLib.makeBeamGhc ghc;

in
beamGhc.shellFor {
  packages = beamLib.beamPackageList;
  buildInputs = with beamGhc; [
    cabal-install
    ghcid
    haskell-language-server
    hpack
  ];
  nativeBuildInputs = [
    postgresql
    sqlite-interactive
  ];
}
