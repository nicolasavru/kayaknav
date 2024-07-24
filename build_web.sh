#!/bin/bash
set -ex

rm -rf target/web
cp -r web target/
wasm-pack -v build . --target web --no-typescript --no-pack --release --out-dir target/web/pkg
sed -i -e "s/%DATE%/$(date +%+4Y%m%d%H%M%S)/" target/web/sw.js
rm target/web/pkg/.gitignore
