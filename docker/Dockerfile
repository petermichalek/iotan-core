FROM ubuntu
# FROM java:openjdk-8

# Define environment variables
ENV SHARE /usr/local/share
ENV SCALA_HOME $SHARE/scala
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle
ENV PATH=$SCALA_HOME/bin:$JAVA_HOME/bin:$PATH

# http://downloads.lightbend.com/scala/2.11.8/scala-2.11.8.tgz

# Move Scala
ADD scala-2.11.8.tgz /

# Install JDK 
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y  software-properties-common && \
    add-apt-repository ppa:webupd8team/java -y && \
    apt-get update && \
    echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections && \
    apt-get install -y oracle-java8-installer && \
    apt-get clean

# Get JDK and Scala into place
RUN mv /scala-2.11.8 $SCALA_HOME

ENV IOTAN_HOME /opt/iotus
ENV CASSANDRA_HOST cassandra

# configuration
# install iotus.yaml, with modification to connect to correct cassandra

ADD iotus-TEMPLATE.yaml $IOTAN_HOME/conf/iotus.yaml

# dependencies don't change frequently
ADD iotan-hs-bin-deps-0.1.01.tar.gz /opt/iotus
# change to dir to be overwritten/added to by subsequent AND/untar
RUN mv /opt/iotus/iotan-hs-bin-deps-0.1.01 /opt/iotus/iotan-hs


# these may change during dev cycle
ADD iotan-hs-bin-0.1.01.tar.gz /opt/iotus/iotan-hs 
# overwrite recursively
RUN cp -rp /opt/iotus/iotan-hs/iotan-hs-bin-0.1.01/* /opt/iotus/iotan-hs && \
    rm -rf /opt/iotus/iotan-hs/iotan-hs-bin-0.1.01/

ADD iotan-core-bin-0.1.01.tar.gz /opt/iotus
RUN mv /opt/iotus/iotan-core-bin-0.1.01 /opt/iotus/iotan-core

#ADD iotan-core-0.1.01 /opt/iotus/iotan-core 


# haystack server will listen on port 1225
EXPOSE 1225

VOLUME /opt/iotus/iotan-hs/logs 

ENTRYPOINT ["/opt/iotus/iotan-core/docker/entrypoint.sh"]

# hs = $IOTAN_HOME/iotan-hs/run.sh
CMD ["hs", "-f"]

