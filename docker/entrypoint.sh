#!/bin/bash
# Perform init of db, if necessary, then invoke hs server
# this file is usually locations in IOTAN_HOME docker location in
# /opt/iotus

# create keyspace iotus (this is for now done separately in cassandra setup)
# $IOTAN_HOME/iotus-core/docker/populate_db.py -v iotus

# create project/populate with data. error will occur if the project "simple"
# already exists, that's OK.

# modify iotus template with cassandra host

# this will run only once, when .bak file doesn't exist
if [ ! -e $IOTAN_HOME/conf/iotus.yaml.bak ]; then
  sed -i.bak "s/CASSANDRA_HOST/$CASSANDRA_HOST/" $IOTAN_HOME/conf/iotus.yaml
fi

if [ "$1" = 'hs' ]; then
    $IOTAN_HOME/iotan-hs/srepl.sh $IOTAN_HOME/iotan-core/docker/project_create.scala simple America/Log_Angeles $IOTAN_HOME/iotan-core/docker/iotus-simple.zinc false
    # start the server
    exec $IOTAN_HOME/iotan-hs/run.sh
else
    exec "$@"
fi


