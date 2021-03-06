#!/bin/bash
# run.sh
# author - Ting Pan
# This script is used to run the spark script

# cd to the word folder
cd "$(dirname "$0")"

CORPUS_PATH="/processed_corpus"
# remove the pre-processed corpus first
~/hadoop/bin/hdfs dfs -rm -r $CORPUS_PATH

# run the spark program
~/spark/bin/spark-submit  \
  --name "15719.Project2" \
  --driver-memory 8g \
  --executor-memory 8g \
  --conf "spark.default.parallelism=$5" \
  ./spark.py $1 $2 $3 $4 $5 $6