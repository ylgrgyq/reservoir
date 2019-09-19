#! /bin/bash

BASE_DIR=$(dirname $0)/..
CLASSPATH=$(echo $BASE_DIR/target/*.jar | tr ' ' ':')

LOG_DIR="$BASE_DIR/logs"
LOG4J_CONFIG="$BASE_DIR/src/main/resources/log4j2.xml"

echo $CLASSPATH

java \
    -Xmx4G -Xms4G \
    -Dlog4j.logdir=$LOG_DIR \
    -Dlog4j.configurationFile=file:$LOG4J_CONFIG \
    -XX:+UseParallelGC \
    -cp $CLASSPATH \
    com.github.ylgrgyq.reservoir.benchmark.storage.BenchmarkBootstrap \
    $@

