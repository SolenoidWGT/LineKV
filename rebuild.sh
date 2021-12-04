#!/bin/bash
#export HOME='/home/gtwang'
if [ ! -f "./build" ];then
rm -rf ./build
fi

if [ ! -f "./bin" ];then
rm -rf ./bin
fi

if [ ! -f "./lib" ];then
rm -rf ./lib
fi

mkdir build &&
cd build &&
cmake .. &&
make