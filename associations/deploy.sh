#!/usr/bin/env sh
mvn clean package
docker cp target/associations-1.0-SNAPSHOT.jar namenode:associations-1.0-SNAPSHOT.jar
docker cp run.sh namenode:custom-run.sh
