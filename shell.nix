with import <nixpkgs> {};

let
  pythonEnv = pkgs.python3.withPackages (p: with p; [
      gunicorn
      psycopg2
  ]);
in 
mkShell {
  name = "CMC Shell";
  description = "NixEnvironment to use with the CMC tools";
  buildInputs = [
    pythonEnv
  ];
}