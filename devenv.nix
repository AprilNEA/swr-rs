{ pkgs, ... }:

{
  packages = [ pkgs.git ];

  languages.rust.enable = true;
}
