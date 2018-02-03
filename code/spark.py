
"""Calculates the word count of the given file.

the file can be local or if you setup cluster.

It can be hdfs file path"""

# Imports

from pyspark import SparkConf, SparkContext
import sys
import os

# Constants
APP_NAME = "Test"
HDFS_WET_DIR = "/common_crawl_wet/"


def extract_docs(path):
    hdfs_exe = "/root/hadoop/bin/hdfs dfs"


def main(sc):
    path = "crawl-data/CC-MAIN-2016-50/segments/1480698540409.8/wet/CC-MAIN-20161202170900-00000-ip-10-31-129-80.ec2.internal.warc.wet.gz"
    new_path = path.replace("/", "_")
    print path


if __name__ == "__main__":

    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # filename = sys.argv[1]
    # Execute Main functionality

    main(sc)
