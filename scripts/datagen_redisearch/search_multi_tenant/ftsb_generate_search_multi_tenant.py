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


def human_format(num):
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    # add more suffixes if you need them
    return "%.0f%s" % (num, ["", "K", "M", "G", "T", "P"][magnitude])


def ft_search_numeric_int(index_name):
    val_from = get_rand_int_v(-1000, 500)
    val_to = get_rand_int_v(val_from + 1)
    condition = "'@numericInt1:[{} {}]".format(val_from, val_to)
    for n in range(2, 11):
        condition = condition + "|@numericInt{}:[{} {}]".format(n, val_from, val_to)
    condition = condition + "'"
    return ["READ", "R1", 1, "FT.SEARCH", index_name, condition, "NOCONTENT"]


SEARCH = "search-idx"
CREATE_IDX = "create-idx"
DROP_IDX = "drop-idx"
INGEST = "ingest-idx"

choices_str = ",".join([SEARCH, INGEST, CREATE_IDX, DROP_IDX])
choices_probability_str = ",".join([str(x) for x in [0.9, 0.09, 0.005, 0.005]])


def generate_ft_create_row(index, schema_dict, prefix=None):

    cmd = [
        "SETUP_WRITE",
        "W1",
        1,
        "FT.CREATE",
        "{index}".format(index=index),
        "ON",
        "JSON",
    ]
    if prefix is not None:
        cmd.extend(["PREFIX", "1", prefix])
    cmd.append("SCHEMA")
    for f, v in schema_dict.items():
        cmd.append(f)
        if "alias" in v:
            cmd.extend(["AS", v["alias"]])
        cmd.append(v["type"])
    return cmd


def use_case_csv_row_to_cmd(docid_str, schema_dict):
    doc = {}
    for field, field_properties in schema_dict.items():
        fieldname = field_properties["alias"]
        fieldtype = field_properties["type"]
        if fieldtype == "TEXT":
            doc[fieldname] = rand_str(5, 20)
        if fieldtype == "TAG":
            doc[fieldname] = rand_str(5, 10)
        if fieldtype == "NUMERIC":
            doc[fieldname] = random.random()

    json_dump = "{}".format(json.dumps(doc))
    cmd = ["WRITE", "W1", 1, "JSON.SET", docid_str, ".", json_dump]
    return docid_str, cmd, doc, json_dump


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="RediSearch FTSB data generator.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--project",
        type=str,
        default="search_and_json",
        help="the project being tested",
    )
    parser.add_argument(
        "--index-prefix",
        type=str,
        default="idx:tenant",
        help="the index name used for search commands",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=12345,
        help="the random seed used to generate random deterministic outputs",
    )
    parser.add_argument(
        "--query-choices",
        type=str,
        default=choices_str,
        help="comma separated list of queries to produce. one of: {}".format(
            choices_str
        ),
    )
    parser.add_argument(
        "--query-choices-probability",
        type=str,
        default=choices_probability_str,
        help="comma separated probability of the list of queries passed via --query-choices. Needs to have the same number of elements as --query-choices",
    )
    parser.add_argument(
        "--index-limit",
        type=int,
        default=100000,
        help="the total indices to generate to be added in the setup stage",
    )
    parser.add_argument(
        "--doc-limit",
        type=int,
        default=1000000,
        help="the total documents to generate to be added in the setup stage",
    )
    parser.add_argument(
        "--max-doc-per-index",
        type=int,
        default=100,
        help="the maximum number of documents to added on each index (can be less)",
    )
    parser.add_argument(
        "--total-benchmark-commands",
        type=int,
        default=1000000,
        help="the total commands to generate to be issued in the benchmark stage",
    )
    parser.add_argument(
        "--number-numeric-fields",
        type=int,
        default=6,
        help="Number of NUMERIC fields in doc",
    )
    parser.add_argument(
        "--number-text-fields",
        type=int,
        default=6,
        help="Number of TEXT fields in doc",
    )
    parser.add_argument(
        "--number-tag-fields",
        type=int,
        default=1,
        help="Number of TAG fields in doc",
    )
    parser.add_argument(
        "--test-name",
        type=str,
        default="multi-tenant-indices-json",
        help="the name of the test",
    )
    parser.add_argument(
        "--test-description",
        type=str,
        default="benchmark showcasing the creation of millions of RediSearch indices while searching and ingesting data.",
        help="the full description of the test",
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
        "--generate-index-commands-only",
        action="store_true",
        help="generate only the index commands",
    )
    parser.add_argument(
        "--temporary-work-dir",
        type=str,
        default="./tmp",
        help="The temporary dir to use as working directory for file download, compression,etc... ",
    )

    args = parser.parse_args()
    schema_dict = {}

    for n in range(1, args.number_numeric_fields + 1):
        fieldname = "$.num_field_{}".format(n)
        alias = "num_field_{}".format(n)
        type = "NUMERIC"
        field_options = []
        schema_dict[fieldname] = {
            "type": type,
            "alias": alias,
            "field_options": field_options,
        }

    for n in range(1, args.number_text_fields + 1):
        fieldname = "$.text_field_{}".format(n)
        alias = "text_field_{}".format(n)
        type = "TEXT"
        field_options = []
        schema_dict[fieldname] = {
            "type": type,
            "alias": alias,
            "field_options": field_options,
        }

    for n in range(1, args.number_tag_fields + 1):
        fieldname = "$.tag_field_{}".format(n)
        alias = "tag_field_{}".format(n)
        type = "TAG"
        field_options = []
        schema_dict[fieldname] = {
            "type": type,
            "alias": alias,
            "field_options": field_options,
        }

    use_case_specific_arguments = del_non_use_case_specific_keys(dict(args.__dict__))
    query_choices = args.query_choices.split(",")
    query_choices_p = [float(x) for x in args.query_choices_probability.split(",")]
    total_benchmark_commands = args.total_benchmark_commands
    # generate the temporary working dir if required
    working_dir = args.temporary_work_dir
    Path(working_dir).mkdir(parents=True, exist_ok=True)
    seed = args.seed
    project = args.project
    test_name = args.test_name
    description = args.test_description
    index_limit_n = args.index_limit
    test_name = "{}-{}_indices".format(human_format(index_limit_n), test_name)
    s3_bucket_name = "benchmarks.redislabs"
    s3_bucket_path = "redisearch/datasets/{}/".format(test_name)
    s3_uri = "https://s3.amazonaws.com/{bucket_name}/{bucket_path}".format(
        bucket_name=s3_bucket_name, bucket_path=s3_bucket_path
    )

    benchmark_output_file = "{test_name}.{project}.commands".format(
        test_name=test_name, project=project
    )
    benchmark_config_file = "{test_name}.{project}.cfg.json".format(
        test_name=test_name, project=project
    )
    bench_fname = "{}.BENCH.csv".format(benchmark_output_file)
    setup_fname = "{}.INGEST.csv".format(benchmark_output_file)
    create_idxs_fname = "{}.FT_CREATE_ONLY_{}_indices.csv".format(
        benchmark_output_file, human_format(index_limit_n)
    )
    drop_idxs_fname = "{}.FT_DROP_ONLY.csv".format(benchmark_output_file)

    ## remove previous files if they exist
    remove_file_if_exists(benchmark_config_file)
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

    benchmark_repetitions_require_teardown_and_resetup = True

    print("-- Benchmark: {} -- ".format(test_name))
    print("-- Description: {} -- ".format(description))

    total_docs = 0

    print("Using random seed {0}".format(args.seed))
    random.seed(args.seed)

    total_docs = args.doc_limit
    doc_ids = []

    create_idxs_csvfile = open(create_idxs_fname, "w", newline="")
    create_idxs_csv_writer = csv.writer(create_idxs_csvfile, delimiter=",")

    index_limit_n = args.index_limit
    general_prefix = args.index_prefix

    max_doc_per_index = args.max_doc_per_index
    _, _, _, json_dump = use_case_csv_row_to_cmd("n/a", schema_dict)
    key_overhead = 100
    avg_doc_size = len(json_dump) + key_overhead
    total_size = total_docs * avg_doc_size
    total_size_human = human_format(total_size)
    print("Total docs {0}".format(total_docs))
    print("Total expected size {0}B".format(total_size_human))

    progress = tqdm(unit="indices", total=index_limit_n)
    for index_n in range(1, index_limit_n + 1):
        index_name = "{}:index_{}".format(general_prefix, index_n)
        index_prefix = "{}:index_{}:doc".format(general_prefix, index_n)
        cmd = generate_ft_create_row(index_name, schema_dict, index_prefix)
        create_idxs_csv_writer.writerow(cmd)
        progress.update()
    progress.close()
    create_idxs_csvfile.close()
    artifacts = [create_idxs_fname]

    if args.generate_index_commands_only is False:

        setup_fname_csvfile = open(setup_fname, "w", newline="")
        setup_csv_writer = csv.writer(setup_fname_csvfile, delimiter=",")

        progress = tqdm(unit="docs", total=total_docs)
        for doc_n in range(0, total_docs):
            index_n = random.randint(1, index_limit_n)
            docid_str = "{}:index_{}:doc{}".format(general_prefix, index_n, doc_n)
            _, cmd, _, _ = use_case_csv_row_to_cmd(docid_str, schema_dict)
            setup_csv_writer.writerow(cmd)
            progress.update()
        progress.close()
        setup_fname_csvfile.close()

        artifacts.append(setup_fname)

    if args.upload_artifacts_s3:
        upload_dataset_artifacts_s3(s3_bucket_name, s3_bucket_path, artifacts)

    print("############################################")
    print("All artifacts generated.")
