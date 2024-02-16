#!/usr/bin/python3

import argparse
import csv
import json
import os
import random

# package local imports
import sys
import uuid

import boto3
from tqdm import tqdm

sys.path.append(os.getcwd() + "/..")

from common_datagen import (
    download_url,
    generate_setup_json,
    compress_files,
    generate_inputs_dict_item,
    humanized_bytes,
    del_non_use_case_specific_keys,
    add_key_metric,
    upload_dataset_artifacts_s3,
    add_deployment_requirements_redis_server_module,
    add_deployment_requirements_benchmark_tool,
    add_deployment_requirements_utilities,
    init_deployment_requirement,
    remove_file_if_exists,
)
from pathlib import Path
import string


def str_to_float_or_zero(entry):
    val = 0.0
    try:
        val = float(entry)
    except ValueError as e:
        pass
    return val


def index_or_none(list, value):
    index = None
    try:
        index = list.index(value)
    except ValueError:
        pass
    return index


def rand_str(minN, maxN):
    return "".join(
        random.choices(
            string.ascii_uppercase + string.digits, k=random.randint(minN, maxN)
        )
    )


def rand_arr(minN, maxN):
    arr = []
    for x in range(1, random.randint(minN, maxN)):
        arr.append(rand_str(3, 10))
    return arr


def rand_numeric_arr(minN, maxN):
    arr = []
    for x in range(1, random.randint(minN, maxN)):
        arr.append(get_rand_int_v())
    return arr


def get_rand_int_v(start_val=-1000, end_val=1000):
    return random.randint(start_val, end_val)


def get_rand_float_v(start_val=-1000.0, end_val=1000.0):
    return random.random() * (end_val - start_val) + start_val


def rand_numeric_float_arr(minN, maxN):
    arr = []
    for x in range(1, random.randint(minN, maxN)):
        arr.append(get_rand_float_v())
    return arr


def use_case_csv_row_to_cmd(doc_id, field_count=10):
    docid_str = "doc:single:{n}".format(n=doc_id)
    cmd = ["WRITE", "W1", 1, "HSET", docid_str]
    cmd.append("id")
    cmd.append(doc_id)
    for n in range(field_count):
        cmd.extend(["field{}".format(n + 1), rand_str(50, 50)])

    return docid_str, cmd


def human_format(num):
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    # add more suffixes if you need them
    return "%.0f%s" % (num, ["", "K", "M", "G", "T", "P"][magnitude])


def ft_search_numeric_int(index_name, max_int):
    val_from = get_rand_int_v(1, max_int)
    condition = "'@id:[{} {}]'".format(val_from, val_from)
    return ["READ", "R1", 1, "FT.SEARCH", index_name, condition]


def ft_search_tag_int(index_name, max_int):
    val_from = get_rand_int_v(1, max_int)
    condition = "'@id:{" + "{}".format(val_from) + "}'"
    return ["READ", "R1", 1, "FT.SEARCH", index_name, condition]


ID_NUMERIC = "numeric"
ID_TAG = "tag"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="RediSearch FTSB data generator.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--project", type=str, default="redisjson", help="the project being tested"
    )
    parser.add_argument(
        "--index-name",
        type=str,
        default="idx:single",
        help="the index name used for search commands",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=12345,
        help="the random seed used to generate random deterministic outputs",
    )
    parser.add_argument(
        "--doc-limit",
        type=int,
        default=1000000,
        help="the total documents to generate to be added in the setup stage",
    )
    parser.add_argument(
        "--total-benchmark-commands",
        type=int,
        default=1000000,
        help="the total commands to generate to be issued in the benchmark stage",
    )
    parser.add_argument(
        "--test-name",
        type=str,
        default="simple_doc",
        help="the name of the test",
    )
    parser.add_argument(
        "--id-type",
        type=str,
        default=ID_TAG,
        help="type of unique id",
    )
    parser.add_argument(
        "--upload-artifacts-s3",
        default=False,
        action="store_true",
        help="uploads the generated dataset files and configuration file to public benchmarks.redislabs bucket. Proper credentials are required",
    )
    parser.add_argument(
        "--upload-artifacts-s3-uncompressed",
        action="store_true",
        help="uploads the generated dataset files and configuration file to public benchmarks.redislabs bucket. Proper credentials are required",
    )
    parser.add_argument(
        "--temporary-work-dir",
        type=str,
        default="./tmp",
        help="The temporary dir to use as working directory for file download, compression,etc... ",
    )

    args = parser.parse_args()
    total_benchmark_commands = args.total_benchmark_commands
    # generate the temporary working dir if required
    working_dir = args.temporary_work_dir
    Path(working_dir).mkdir(parents=True, exist_ok=True)
    seed = args.seed
    project = args.project
    doc_limit = args.doc_limit
    test_name = args.test_name
    index_name = args.index_name
    id_type = args.id_type
    test_name = "{}-{}-id_{}".format(human_format(doc_limit), test_name, id_type)
    s3_bucket_name = "benchmarks.redislabs"
    s3_bucket_path = "redisearch/datasets/{}/".format(test_name)
    s3_uri = "https://s3.amazonaws.com/{bucket_name}/{bucket_path}".format(
        bucket_name=s3_bucket_name, bucket_path=s3_bucket_path
    )

    benchmark_output_file = "{test_name}.{project}.commands".format(
        test_name=test_name, project=project
    )
    bench_fname = "{}.BENCH.csv".format(benchmark_output_file)
    setup_fname = "{}.SETUP.csv".format(benchmark_output_file)

    ## remove previous files if they exist
    remove_file_if_exists(bench_fname)
    remove_file_if_exists(setup_fname)

    used_indices = []
    setup_commands = []
    teardown_commands = []
    key_metrics = []

    total_writes = 0
    total_reads = 0
    total_updates = 0
    total_deletes = 0

    json_version = "0.1"
    benchmark_repetitions_require_teardown_and_resetup = True

    print("-- Benchmark: {} -- ".format(test_name))

    total_docs = 0

    print("Using random seed {0}".format(args.seed))
    random.seed(args.seed)

    total_docs = 0
    doc_ids = []

    progress = tqdm(unit="docs", total=doc_limit)
    all_csvfile = open(setup_fname, "a", newline="")
    all_csv_writer = csv.writer(all_csvfile, delimiter=",")
    for row_n in range(1, doc_limit + 1):
        docid, cmd = use_case_csv_row_to_cmd(row_n)
        all_csv_writer.writerow(cmd)
        progress.update()
        doc_ids.append(docid)
    progress.close()
    all_csvfile.close()
    progress = tqdm(unit="docs", total=total_benchmark_commands)
    all_csvfile = open(bench_fname, "a", newline="")
    all_csv_writer = csv.writer(all_csvfile, delimiter=",")
    len_docs = len(doc_ids)
    row_n = 0
    while row_n < total_benchmark_commands:
        if id_type == ID_NUMERIC:
            cmd = ft_search_numeric_int(index_name, len_docs)
        elif id_type == ID_TAG:
            cmd = ft_search_tag_int(index_name, len_docs)
        row_n = row_n + 1
        all_csv_writer.writerow(cmd)
        progress.update()
    progress.close()
    all_csvfile.close()

    if args.upload_artifacts_s3:
        artifacts = [setup_fname, bench_fname]
        upload_dataset_artifacts_s3(s3_bucket_name, s3_bucket_path, artifacts)

    print("############################################")
    print("All artifacts generated.")

    create_cmd = (
        "FT.CREATE {} ON HASH PREFIX 1 doc:single SCHEMA id {} SORTABLE".format(
            index_name, id_type
        )
    )
    for n in range(1, 11):
        create_cmd = create_cmd + " field{} TEXT SORTABLE".format(n)
    print("FT.CREATE command:{}".format(create_cmd))
