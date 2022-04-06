#!/bin/bash
export TMP_DIR="`pwd`"
if [ ! -f "./build" ];
then
rm -rf ./build
fi

if [ ! -f "./bin" ];
then
rm -rf ./bin
fi

if [ ! -f "./lib" ];
then
rm -rf ./lib
fi

mkdir build &&
cd build &&
NDEBUG=no NLOCAL=no NCLIENT=yes NSERVER=yes cmake .. &&
make &&
set -x &&
chown -R gtwang "$TMP_DIR"/bin/*""