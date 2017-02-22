#!/usr/bin/env bash
# Copyright © 2017 Peter Michalek
#
# Run scala REPL standalone with all jars in classpath
#
# absolute path for jars is necessary when executing a script which invokes fsc
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#echo $DIR
#LIBDIR=lib
LIBDIR=$DIR/target/pack/lib
CP=
for f in $LIBDIR/*.jar ; do CP=$CP:$f; done
echo $CP
scala -cp $CP $@
