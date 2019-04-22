#!/bin/bash

 nohup spark-submit target/Reval.jar 0 1>/data/logs/re0.log 2>&1 &
 nohup spark-submit target/Reval.jar 1 1>/data/logs/re1.log 2>&1 &
 nohup spark-submit target/Reval.jar 2 1>/data/logs/re2.log 2>&1 &
 nohup spark-submit target/Reval.jar 3 1>/data/logs/re3.log 2>&1 &
 nohup spark-submit target/Reval.jar 4 1>/data/logs/re4.log 2>&1 &
 nohup spark-submit target/Reval.jar 5 1>/data/logs/re5.log 2>&1 &

