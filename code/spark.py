# spark.py 
# author - Ting Pan
# This file contain the function which process the document and do the statics

# Imports
from pyspark import SparkConf, SparkContext, StorageLevel
import sys
import os
import errno
import warc
import gzip
import re

# Global Constants
# program argument
PARAMS = {}

# tmp dictionary for store tmp wet file from hdfs
TMP_DIR = "/tmp"

# default partition size
PARTITION_SIZE = 16

# corpus save path
CORPUS_PATH = "/processed_corpus"

# document length threadholder after screen invalid english words
AFTER_VALID_THREAD = 10

# document length threadholder after screen stop and domain words
AFTER_SCREEN_THREAD = 0

##############################################################
#         Functions used for MAP and REDUCE operation        #
##############################################################


# Extract the docs from the give wet path
#       - input: wet file path(String)
#       - return: list of payload ([String])
def extract_docs(path):
    # concate path
    hdfs_exe = "/root/hadoop/bin/hdfs dfs"
    wet_file_name = path.replace("/", "_")
    hdfs_wet_path = os.path.join(PARAMS["data_path"], wet_file_name)
    tmp_wet_path = os.path.join(TMP_DIR, wet_file_name)

    # make folder and download
    os.system("mkdir " + TMP_DIR)
    os.system(hdfs_exe + " -get " + hdfs_wet_path + " " + tmp_wet_path)

    # open wet obj
    gzip_fobj = gzip.open(tmp_wet_path, "r")
    warc_fobj = warc.WARCFile(fileobj=gzip_fobj, compress=False)
    doc_list = []

    while True:
        try:
            record = warc_fobj.read_record()
            if record is None:
                break
            if record["WARC-Type"] == "conversion":
                doc_list.append(record.payload.read())
        except:
            pass
    
    # remove tmp file
    os.system("rm " + tmp_wet_path)
    return doc_list


# Count the document frequency for each word
#       - input: word list of each document
#       - return: [(word, 1) for each word in the document]
def count_doc_freq(word_list):
    result = []
    visited = set()
    for w in word_list:
        if w not in visited:
            visited.add(w)
            result.append((w, 1))
    return result


# Count the document frequency and the words count for each word
#       - input: word list of each document
#       - return: [(word, [word_count, 1]) for each word in the document]
def count_doc_term_freq(word_list):
    counted_word = {}

    # count the document freq and word count within one doc
    for word in word_list:
        if word not in counted_word:
            counted_word[word] = 1
        else:
            counted_word[word] = counted_word[word] + 1

    return list(map(lambda x: (x, [counted_word[x], 1]), counted_word))


# Remove invalid english word for each document
#       - input: document
#       - return: [word for each valid english]
def remove_invalid_token(document):
    
    # split the word by whitespace
    words = document.split()

    # result list
    valid_words = []

    # regexp for valid english word
    pattern = re.compile("^[-'\"]*[a-zA-Z\-']+[-'\"]*$")

    for w in words:
        if pattern.match(w):
            trip_w = w.strip("-'\"")
            if len(trip_w) > 0:
                valid_words.append(trip_w.lower())
    return valid_words


# Generate the statics fact for each document, used as map function
#       - input: (document_length, document_index)
#       - return: dict with statics
def gen_statics(tuple, total_docs):

    return {
        "max_len": tuple[0],
        "min_len": tuple[0],
        "tokens": tuple[0],
        "10th": tuple[0] if tuple[1] == int(0.1 * total_docs) else -1,
        "30th": tuple[0] if tuple[1] == int(0.3 * total_docs) else -1,
        "50th": tuple[0] if tuple[1] == int(0.5 * total_docs) else -1,
        "70th": tuple[0] if tuple[1] == int(0.7 * total_docs) else -1,
        "90th": tuple[0] if tuple[1] == int(0.9 * total_docs) else -1,
        "99th": tuple[0] if tuple[1] == int(0.99 * total_docs) else -1,
    }


# Merge the statics fact for each document, used for reduce function
#       - input: (static_fact_x, static_fact_y)
#       - return: merged dict with statics
def merge_statics(x, y):

    return {
        "max_len": max(x["max_len"], y["max_len"]), 
        "min_len": min(x["min_len"], y["min_len"]),
        "tokens": x["tokens"] + y["tokens"],
        "10th": x["10th"] if y["10th"] == -1 else y["10th"],
        "30th": x["30th"] if y["30th"] == -1 else y["30th"],
        "50th": x["50th"] if y["50th"] == -1 else y["50th"],
        "70th": x["70th"] if y["70th"] == -1 else y["70th"],
        "90th": x["90th"] if y["90th"] == -1 else y["90th"],
        "99th": x["99th"] if y["99th"] == -1 else y["99th"]
    }


# Write the result in the given ouput folder
#       - input: (static_dict, frequency_list)
def write_result(statics, freq):
    
    # make the output folder
    os.system("mkdir " + PARAMS["output_dir"])

    # concate path
    dict_path = os.path.join(PARAMS["output_dir"], "dictionary")
    freq_path = os.path.join(PARAMS["output_dir"], "frequency")
    stat_path = os.path.join(PARAMS["output_dir"], "statistics")

    # write the word freq and dictionary
    dictionary = open(dict_path, "w+")
    frequency = open(freq_path, "w+")

    linum = 0
    for (word, freq) in freq:
        frequency.write("{}\t{}\t{}\n".format(word, freq[0], freq[1]))
        dictionary.write("{}\t{}\n".format(word, linum))
        linum += 1

    dictionary.close()
    frequency.close()

    # write the statistics data
    statistics = open(stat_path, "w+")
    statistics.write("Number of Words: {}\n".format(statics["words"]))
    statistics.write("Number of Documents: {}\n".format(statics["docs"]))
    statistics.write("Number of Tokens: {}\n".format(statics["tokens"]))
    statistics.write("Maximum Document Length: {}\n".format(statics["max_len"]))
    statistics.write("Minimum Document Length: {}\n".format(statics["min_len"]))
    statistics.write("Average Document Length: {}\n".format(statics["avg_len"]))
    statistics.write(
        "Document Length 10th percentile: {}\n".format(statics["10th"]))
    statistics.write(
        "Document Length 30th percentile: {}\n".format(statics["30th"]))
    statistics.write(
        "Document Length 50th percentile: {}\n".format(statics["50th"]))
    statistics.write(
        "Document Length 70th percentile: {}\n".format(statics["70th"]))
    statistics.write(
        "Document Length 90th percentile: {}\n".format(statics["90th"]))
    statistics.write(
        "Document Length 99th percentile: {}\n".format(statics["99th"]))
    statistics.close()


##############################################################
#             Functions used for generate each RDD           #
##############################################################


# Extract the docs from the give wet path list
#       - input: wet file path(String)
#       - return: rdd with format [doc]
def gen_doc_from_name(sc, name_list):
    return (sc.parallelize(name_list, PARTITION_SIZE)
                .flatMap(extract_docs)
                .repartition(PARTITION_SIZE))

# Remove the invalid english word from docs
#       - input: rdd with format [doc]
#       - return: rdd with format [[word of each doc]] 
def gen_screen_invalid_word(rdd, bc_stop_set):
    return (rdd.map(remove_invalid_token)
                .filter(lambda x: len(x) > AFTER_VALID_THREAD)
                .repartition(PARTITION_SIZE)
                .map(lambda doc: [w for w in doc if w not in bc_stop_set.value])
                .cache())

# Generate the domain set from rdd and total document
#       - input: rdd with format [[word of each doc]], document_count
#       - return: set(domain_word)
def gen_domian_set(rdd, doc_count):
    res_rdd = (rdd.flatMap(count_doc_freq)
                .reduceByKey(lambda x, y: x + y)
                .filter(lambda x: x[1] >= 0.9 * doc_count or x[1] < PARAMS["low_freq"])
                .map(lambda x: x[0]))
    return set(res_rdd.collect())

# Remove the domain set from the word list
#       - input: rdd with format [[word of each doc]]
#       - return: rdd with format [[word of each doc]]
def gen_screen_domian_set(rdd, bc_domain_set):
    return (rdd.map(lambda doc: [w for w in doc if w not in bc_domain_set.value])
                .filter(lambda x: len(x) > AFTER_SCREEN_THREAD)
                .cache())        

# Generate the frequency rdd sorted with alphabet order
#       - input: rdd with format [(word, [word_count, doc_freq])]
#       - return: sorted rdd with format [(word, [word_count, doc_freq])]
def gen_sorted_frequency(rdd):
    return (rdd.flatMap(lambda x: x)
                .reduceByKey(lambda x, y: [x[0] + y[0], x[1] + y[1]])
                .sortByKey(True)
                .cache())

# Generate the word => index map  
#       - input: rdd with format [((word, [word_count, doc_freq]), index)]
#       - return: sorted rdd with format {(word, index)} 
def gen_word_map(rdd):
    return (rdd.zipWithIndex()
                .map(lambda x: (x[0][0], x[1]))
                .collectAsMap())

# Generate the process corups from doc
#       - input: rdd with format [(word, [word_count, doc_freq])]
#       - return: [(word_index, word_count)]
def gen_corups(doc, bc_word_dict):
    corups_list = map(lambda x: "{}:{}".format(bc_word_dict.value[x[0]], x[1][0]), doc)
    return ",".join(corups_list)

# Generate the document length statics from the doc
#       - input: rdd with format [word_list]
#       - return: {static_result}
def gen_length_statics(rdd, total_docs):
    return (rdd.map(lambda x: len(x))
                .sortBy(lambda x: x)
                .zipWithIndex()
                .map(lambda x: gen_statics(x, total_docs))
                .reduce(merge_statics))

# The main logic which process the document and generate the result
def do_statics(sc):
    # init the result dictionary
    statics = {}

    # init the wet file and stop word set from the PARAMS
    wet_list = open(PARAMS["name_file"]).read().splitlines()
    stop_set = set(open(PARAMS["stop_words"]).read().splitlines())

    # generate the doc rdd from the function
    pure_doc_rdd = gen_doc_from_name(sc, wet_list)

    # remove invalid english word and stop words
    bc_stop_set = sc.broadcast(stop_set)
    valid_eng_rdd = gen_screen_invalid_word(pure_doc_rdd, bc_stop_set)
    
    # generate the domain specific and low freq word set
    doc_count = valid_eng_rdd.count()
    domain_set = gen_domian_set(valid_eng_rdd, doc_count)

    # remove domain and low freq words
    bc_domain_set = sc.broadcast(domain_set)
    screen_domain_rdd = gen_screen_domian_set(valid_eng_rdd, bc_domain_set)

    # count the document count
    statics["docs"] = screen_domain_rdd.count()

    # count the term frequency and the document frequency.
    word_freq_rdd = screen_domain_rdd.map(count_doc_term_freq).cache()
    sorted_freq_rdd = gen_sorted_frequency(word_freq_rdd)
    
    # collect the word => index map
    word_map = gen_word_map(sorted_freq_rdd)

    # collect the frequency result from rdd
    freq_list = sorted_freq_rdd.collect()
    statics["words"] = len(freq_list)

    bc_word_map = sc.broadcast(word_map)
    # save the processed corups
    word_freq_rdd.map(lambda x : gen_corups(x, bc_word_map)).saveAsTextFile(CORPUS_PATH)

    # statics the document length
    len_stat = gen_length_statics(screen_domain_rdd, statics["docs"])

    # merge all result
    statics = dict(statics.items() + len_stat.items())
    statics["avg_len"] = int(statics["tokens"] / statics["docs"])

    # write the result to the local dictionary
    write_result(statics, freq_list)


if __name__ == "__main__":
    
    # read the user input
    PARAMS = {
        "data_path": sys.argv[1],
        "name_file": sys.argv[2],
        "stop_words": sys.argv[3],
        "low_freq": int(sys.argv[4]),
        "cores": int(sys.argv[5]),
        "output_dir": sys.argv[6]
    }
    
    # set the default partition size with number of cores
    PARTITION_SIZE = PARAMS["cores"]

    # get sc context
    sc = sc = SparkContext()

    # do the statics
    do_statics(sc)

    print "Finish all jobs!"
