import sys
import os
import re
import gzip
import warc


def extract_docs(wet_file):
    gzip_fobj = gzip.open(wet_file, "r")
    warc_fobj = warc.WARCFile(fileobj=gzip_fobj, compress=False)
    doc_list = []
    while True:
        record = warc_fobj.read_record()
        if record is None:
            break
        if record["WARC-Type"] == "conversion":
            document = record.payload.read()
            valid_words = remove_invalid_token(document)
            if len(valid_words) >= 10:
                doc_list.append(valid_words)
        else:
            print record["WARC-Type"]
    return doc_list


def remove_invalid_token(document):
    words = document.split()
    pattern = re.compile("^([a-zA-Z\-']+|\"[a-zA-Z\-']+\")$")
    valid_words = [w for w in words if pattern.match(w)]
    return valid_words


def count_freq(words):
    d = dict()
    for w in words:
        if w in d:
            d[w] = (d[w][0] + 1, 1)
        else:
            d[w] = (1, 1)
    return d


def main():
    print sys.argv
    doc_list = extract_docs(
        "../data/CC-MAIN-20161202170900-00000-ip-10-31-129-80.ec2.internal.warc.wet.gz")
    print(doc_list)
    print(len(doc_list))
    # doc_list.map()


if __name__ == "__main__":
    main()
