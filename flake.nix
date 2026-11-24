{
  description = "rdma_gen - RDMA traffic generator";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forAll = f: nixpkgs.lib.genAttrs systems (s: f nixpkgs.legacyPackages.${s});

      # CMakeLists.txt requires GCC >= 13 (folly/coro does not build with clang)
      mkDeps = pkgs: with pkgs; [
        boost
        fast-float
        fbthrift
        fizz
        fmt
        folly
        gflags
        gtest
        glog
        libevent
        double-conversion
        openssl
        rdma-core
        re2
        wangle
        zlib
        zstd
      ];
      # Sources rely on gflags/fmt being included transitively by the getdeps-pinned
      # folly; nixpkgs' folly no longer does that.
      extraCxxFlags = "-include gflags/gflags.h -include fmt/format.h";
    in
    {
      packages = forAll (pkgs: {
        rdma_gen = pkgs.stdenv.mkDerivation {
          pname = "rdma_gen";
          version = "git";
          src = self;
          nativeBuildInputs = with pkgs; [ cmake ninja pkg-config python3 ];
          buildInputs = mkDeps pkgs;
          cmakeFlags = [ "-DCMAKE_BUILD_TYPE=Release" ];
          env.CXXFLAGS = extraCxxFlags;
          installPhase = ''
            runHook preInstall
            install -Dm755 bin/rdma_gen $out/bin/rdma_gen
            runHook postInstall
          '';
        };
        default = self.packages.${pkgs.stdenv.hostPlatform.system}.rdma_gen;
      });

      devShells = forAll (pkgs: {
        default = pkgs.mkShell {
          inputsFrom = [ self.packages.${pkgs.stdenv.hostPlatform.system}.rdma_gen ];
          packages = with pkgs; [ gdb cmake-format ];
          CXXFLAGS = extraCxxFlags;
        };
      });
    };
}
