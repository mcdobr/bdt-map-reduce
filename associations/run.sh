#!/usr/bin/env sh
mkdir -p input

hadoop fs -mkdir -p input
hdfs dfs -put ./input/* input

hadoop jar associations-1.0-SNAPSHOT.jar me.mircea.associations.Associations /user/root/input /user/root/output
