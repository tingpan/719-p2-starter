
"""Calculates the word count of the given file.

the file can be local or if you setup cluster.

It can be hdfs file path"""

# Imports

from pyspark import SparkConf, SparkContext, StorageLevel
import sys
import os, errno
import warc
import gzip
import re
import shutil

# Constants
APP_NAME = "Test"
HDFS_WET_DIR = "/common_crawl_wet/"
LOCAL_DIR = "./local_crawl_wet/"
PARTITION_SIZE = 64
LOW_FREQ = 10

def extract_docs(wet_file_name):
    hdfs_exe = "/root/hadoop/bin/hdfs dfs"

    try:
        os.makedirs(LOCAL_DIR)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    os.system(hdfs_exe + " -get " + HDFS_WET_DIR +
              wet_file_name + " " + LOCAL_DIR + wet_file_name)
    wet_file = LOCAL_DIR + wet_file_name
    gzip_fobj = gzip.open(wet_file, "r")
    warc_fobj = warc.WARCFile(fileobj=gzip_fobj, compress=False)
    doc_list = []

    while True:
        record = warc_fobj.read_record()
        if record is None:
            break
        if record["WARC-Type"] == "conversion":
            document = record.payload.read()
            doc_list.append(document)
        else:
            print record["WARC-Type"]

    shutil.rmtree(LOCAL_DIR, ignore_errors=True)
    return doc_list

def count_doc_freq(word_list):
    return list(map(lambda x: (x, 1), set(word_list)))

def count_doc_term_freq(word_list):
    counted_word={}
    for word in word_list:
      if word not in counted_word:
        counted_word[word] = 1
    else:
        counted_word[word] += 1
    return list(map(lambda x: (x, [counted_word[x], 1]), counted_word))

def remove_set_word(word_list, remove_set):
    print(len(word_list))
    return [w for w in word_list if w not in remove_set]

def remove_invalid_token(document):
    words = document.split()
    valid_words = []
    pattern = re.compile("(^[a-zA-Z\-']+$)|(^[-'\"]+[a-zA-Z\-']+[-'\"]+$)") 

    for w in words:
        if pattern.match(w):
            trip_w = w.strip("-'\"")
            if len(trip_w) > 0:
                valid_words.append(trip_w.lower())

    return valid_words



def main(sc):
    stop_set = set(open("../stop_words").read().splitlines())
    path = "crawl-data/CC-MAIN-2016-50/segments/1480698540409.8/wet/CC-MAIN-20161202170900-00000-ip-10-31-129-80.ec2.internal.warc.wet.gz"
    wet_file_name = path.replace("/", "_")
    path_list = [wet_file_name]

    rdd0 = sc.parallelize(path_list).flatMap(extract_docs)
    print(rdd0.count())

    rdd1 = rdd0.repartition(PARTITION_SIZE).map(remove_invalid_token).filter(lambda x: len(x) > 10).map(lambda x: [w for w in x if w not in stop_set])
    doc_count = rdd1.count()
    print(doc_count)

    rdd3 = rdd1.flatMap(count_doc_freq).repartition(PARTITION_SIZE).reduceByKey(lambda x, y : x + y)
    print(rdd3.count())

    domain_set = set(rdd3.repartition(PARTITION_SIZE).filter(lambda x : x[1] >= 0.9 * doc_count or x[1] < LOW_FREQ).map(lambda x: x[0]).collect())

    rdd4 = rdd1.map(lambda x: remove_set_word(x, domain_set)).filter(lambda x: len(x) > 0)
    print(rdd4.count())

    rdd5 = rdd4.flatMap(count_doc_term_freq).repartition(PARTITION_SIZE).reduceByKey(lambda x, y : [x[0] + y[0], x[1] + y[1]])
    print(rdd5.count())

if __name__ == "__main__":

    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("spark://master:7077")
    sc = SparkContext(conf=conf)

    # filename = sys.argv[1]
    # Execute Main functionality

    main(sc)
    raw_input("Press [ANY] Key to Continue...")
    sc.stop()
