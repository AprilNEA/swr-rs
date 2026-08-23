{ pkgs, ... }:

{
  # lld links the wasm32-unknown-unknown test binaries (swr-runtime-web).
  packages = [
    pkgs.git
    pkgs.lld
  ];

  # Stable toolchain with the wasm target so clippy/check work on both
  # native and wasm32-unknown-unknown (spec 3.1).
  languages.rust = {
    enable = true;
    channel = "stable";
    targets = [ "wasm32-unknown-unknown" ];
  };
}
