#!/usr/bin/env bash
# Copyright © 2017 Peter Michalek
#
# Pack binaries for docker
# sbt pack must have been run before this.
#version := "0.1.01"
#version=0.1.01
version=$(sed s/version:=// ./target/pack/VERSION)
BINDIR=target/iotan-core-bin-$version
TARNAME=iotan-core-bin-$version.tar.gz

# first cleanup from previous build
for f in $BINDIR; do
  rm -rf $f
done

mkdir $BINDIR
mkdir -p $BINDIR/db/jars
mkdir -p $BINDIR/docker/dev
mkdir $BINDIR/lib

cp -a db/*.cql $BINDIR/db
#cp -a db/jars/* $BINDIR/db/jars
cp -a docker/entrypoint.sh $BINDIR/docker
cp -a docker/populate_db.py $BINDIR/docker
cp -a src/main/resources/iotus-simple.zinc $BINDIR/docker
cp -a docker/project_create.scala $BINDIR/docker
cp -a docker/dev/*.cql $BINDIR/docker/dev
cp -a srepl.sh $BINDIR
cp -a requirements.txt $BINDIR
cp -a target/pack/VERSION $BINDIR
cp -a ./target/scala-2.11/*.jar $BINDIR/lib
#cp -a target/pack/* $BINDIR
cd target
tar zcvf ../$TARNAME ./iotan-core-bin-$version
cd ..
echo A new file was created in ./$TARNAME
ls -l $TARNAME

