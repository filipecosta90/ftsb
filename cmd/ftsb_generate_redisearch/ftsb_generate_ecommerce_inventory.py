import argparse
import csv
import json
import random
import re
import time
import uuid

# package local imports
import boto3 as boto3
from common import generate_setup_json, compress_files
from tqdm import tqdm


def process_inventory(row, market_count, nodes, total_nodes, docs_map, product_ids, countries_alpha_3,
                      countries_alpha_p):
    # uniq_id,product_name,manufacturer,price,number_available_in_stock,number_of_reviews,number_of_answered_questions,average_review_rating,amazon_category_and_sub_category,customers_who_bought_this_item_also_bought,description,product_information,product_description,items_customers_buy_after_viewing_this_item,customer_questions_and_answers,customer_reviews,sellers
    added_docs = 0
    NUMERIC = "NUMERIC"
    GEO = "GEO"
    TAG = "TAG"
    TEXT = "TEXT"
    for inner_doc_pos in range(0, market_count):
        skuId = row[0]
        brand = row[2]
        sellers_raw = row[16]
        nodeType = "store"
        availableToSource = "true"
        standardAvailableToPromise = "true"
        bopisAvailableToPromise = "true"
        onHold = "false"
        exclusionType = "false"

        onhand = random.randint(0, 64000)
        allocated = random.randint(0, 64000)
        reserved = random.randint(0, 64000)
        storeAllocated = random.randint(0, 64000)
        transferAllocated = random.randint(0, 64000)
        storeReserved = random.randint(0, 64000)
        confirmedQuantity = random.randint(0, 64000)
        standardSafetyStock = random.randint(0, 64000)
        bopisSafetyStock = random.randint(0, 64000)
        virtualHold = random.randint(0, 64000)

        onhandLastUpdatedTimestamp = int(time.time() + random.randint(0, 24 * 60 * 60))
        allocatedLastUpdatedTimestamp = int(time.time() + random.randint(0, 24 * 60 * 60))
        reservedLastUpdatedTimestamp = int(time.time() + random.randint(0, 24 * 60 * 60))
        storeAllocatedLastUpdatedTimestamp = int(time.time() + random.randint(0, 24 * 60 * 60))
        transferAllocatedLastUpdatedTimestamp = int(time.time() + random.randint(0, 24 * 60 * 60))
        storeReservedLastUpdatedTimestamp = int(time.time() + random.randint(0, 24 * 60 * 60))

        pattern = re.compile('[\W_]+')

        sellers = re.findall(
            r'\"Seller_name_\d+\"=>\"([^"]+)\"', sellers_raw)
        if len(sellers) == 0:
            available = "false"

        for node in sellers:
            if node not in nodes:
                total_nodes = total_nodes + 1
                nodeId = total_nodes
                nodes[node] = nodeId

        nodesList = list(nodes.keys())
        if len(nodesList) > 0:
            # k = 5 if 5 <= len(nodesList) else len(nodesList)
            k = 10
            for node in random.choices(nodesList, k=k):
                # print(random.choices(nodesList, k=k))
                nodeId = nodes[node]
                did = str(uuid.uuid4()).replace("-", "")
                if skuId not in product_ids:
                    product_ids[skuId] = 1
                else:
                    product_ids[skuId] += 1
                market = random.choices(countries_alpha_3, weights=countries_alpha_p)[0]
                doc_id = "{market}_{nodeId}_{skuId}".format(market=market, nodeId=nodeId, skuId=did)

                if doc_id not in docs_map:
                    doc = {"doc_id": doc_id,
                           "schema": {
                               "market": {"type": TAG, "value": market, "field_options": ["SORTABLE"]},
                               "nodeId": {"type": TAG, "value": nodeId, "field_options": ["SORTABLE"]},
                               "skuId": {"type": TAG, "value": skuId,
                                         "field_options": ["SORTABLE"]},
                               # onhand
                               "onhand": {"type": NUMERIC, "value": onhand,
                                          "field_options": ["SORTABLE", "NOINDEX"]},
                               "onhandLastUpdatedTimestamp": {"type": NUMERIC, "value": onhandLastUpdatedTimestamp,
                                                              "field_options": ["SORTABLE", "NOINDEX"]},
                               # allocated
                               "allocated": {"type": NUMERIC, "value": allocated,
                                             "field_options": ["SORTABLE", "NOINDEX"]},
                               "allocatedLastUpdatedTimestamp": {"type": NUMERIC,
                                                                 "value": allocatedLastUpdatedTimestamp,
                                                                 "field_options": ["SORTABLE", "NOINDEX"]},
                               # reserved
                               "reserved": {"type": NUMERIC, "value": reserved,
                                            "field_options": ["SORTABLE", "NOINDEX"]},
                               "reservedLastUpdatedTimestamp": {"type": NUMERIC, "value": reservedLastUpdatedTimestamp,
                                                                "field_options": ["SORTABLE", "NOINDEX"]},
                               # store allocated
                               "storeAllocated": {"type": NUMERIC, "value": storeAllocated,
                                                  "field_options": ["SORTABLE", "NOINDEX"]},
                               "storeAllocatedLastUpdatedTimestamp": {"type": NUMERIC,
                                                                      "value": storeAllocatedLastUpdatedTimestamp,
                                                                      "field_options": ["SORTABLE", "NOINDEX"]},
                               # transfer allocated
                               "transferAllocated": {"type": NUMERIC, "value": transferAllocated,
                                                     "field_options": ["SORTABLE", "NOINDEX"]},
                               "transferAllocatedLastUpdatedTimestamp": {"type": NUMERIC,
                                                                         "value": transferAllocatedLastUpdatedTimestamp,
                                                                         "field_options": ["SORTABLE", "NOINDEX"]},

                               # transfer allocated
                               "storeReserved": {"type": NUMERIC, "value": storeReserved,
                                                 "field_options": ["SORTABLE", "NOINDEX"]},
                               "storeReservedLastUpdatedTimestamp": {"type": NUMERIC,
                                                                     "value": storeReservedLastUpdatedTimestamp,
                                                                     "field_options": ["SORTABLE", "NOINDEX"]},

                               # store reserved
                               "confirmedQuantity": {"type": NUMERIC, "value": confirmedQuantity,
                                                     "field_options": ["SORTABLE", "NOINDEX"]},
                               "standardSafetyStock": {"type": NUMERIC, "value": standardSafetyStock,
                                                       "field_options": ["SORTABLE", "NOINDEX"]},
                               "bopisSafetyStock": {"type": NUMERIC, "value": bopisSafetyStock,
                                                    "field_options": ["SORTABLE", "NOINDEX"]},
                               "virtualHold": {"type": NUMERIC, "value": virtualHold,
                                               "field_options": ["SORTABLE", "NOINDEX"]},

                               # tags
                               "availableToSource": {"type": TAG, "value": pattern.sub('', availableToSource),
                                                     "field_options": []},
                               "standardAvailableToPromise": {"type": TAG,
                                                              "value": pattern.sub('', standardAvailableToPromise),
                                                              "field_options": []},
                               "bopisAvailableToPromise": {"type": TAG,
                                                           "value": pattern.sub('', bopisAvailableToPromise),
                                                           "field_options": []},

                               "nodeType": {"type": TAG, "value": pattern.sub('', nodeType), "field_options": []},
                               "brand": {"type": TAG, "value": pattern.sub('', brand), "field_options": ["NOINDEX"]},

                               "onHold": {"type": TAG, "value": pattern.sub('', onHold), "field_options": []},
                               "exclusionType": {"type": TAG, "value": pattern.sub('', exclusionType),
                                                 "field_options": []},
                           }
                           }
                    docs_map[doc_id] = doc
                    added_docs = added_docs + 1

    return nodes, total_nodes, docs_map, added_docs, product_ids


def generate_ft_aggregate_row(index, countries_alpha_3, countries_alpha_p, maxSkusList, skus, maxNodesList, nodes):
    product_id_list = []
    market = random.choices(countries_alpha_3, weights=countries_alpha_p)[0]

    skuId_list = random.choices(skus, k=maxSkusList)
    nodeId_list = random.choices(nodes, k=maxNodesList)

    cmd = ["READ", "FT.AGGREGATE", "{index}".format(index=index),
           "@market:{{{0}}} @skuId:{{{1}}} @nodeId:{{{2}}}".format(market,
                                                                   "|".join(skuId_list),
                                                                   "|".join(nodeId_list))
        , "LOAD", "21", "@market", "@skuId", "@nodeId", "@brand", "@nodeType", "@onhand", "@allocated",
           "@confirmedQuantity", "@reserved", "@virtualHold", "@availableToSource", "@standardAvailableToPromise",
           "@bopisAvailableToPromise", "@storeAllocated", "@bopisSafetyStock", "@transferAllocated",
           "@standardSafetyStock", "@storeReserved", "@availableToSource", "@exclusionType", "@onHold", "WITHCURSOR",
           "COUNT", "500"]
    return cmd


def generate_ft_add_row(index, doc):
    cmd = ["SETUP_WRITE", "FT.ADD", "{index}".format(index=index),
           "{index}-{doc_id}".format(index=index, doc_id=doc["doc_id"]), 1.0, "REPLACE", "FIELDS"]
    for f, v in doc["schema"].items():
        cmd.append(f)
        cmd.append(v["value"])
    return cmd


def generate_ft_create_row(index, doc):
    cmd = ["FT.CREATE", "{index}".format(index=index), "SCHEMA"]
    for f, v in doc["schema"].items():
        cmd.append(f)
        cmd.append(v["type"])
        if len(v["field_options"]) > 0:
            cmd.extend(v["field_options"])
    return cmd


def generate_ft_add_update_row(indexname, doc):
    cmd = ["UPDATE", "FT.ADD", "{index}".format(index=indexname),
           "{index}-{doc_id}".format(index=indexname, doc_id=doc["doc_id"]), 1.0,
           "REPLACE", "PARTIAL", "FIELDS"]
    TRUES = "true"
    FALSES = "false"
    standardAvailableToPromise = TRUES if bool(random.getrandbits(1)) == True else FALSES
    availableToSource = TRUES if bool(random.getrandbits(1)) == True else FALSES
    market = doc["schema"]["market"]["value"]
    nodeId = doc["schema"]["nodeId"]["value"]
    nodeType = doc["schema"]["nodeType"]["value"]
    new = [
        "market", market, "nodeId", nodeId, "nodeType", nodeType, "availableToSource", availableToSource,
        "standardAvailableToPromise", standardAvailableToPromise]
    cmd.extend(new)

    return cmd


def generate_setup_commands():
    global progress, csvfile, nodes, total_nodes, docs_map, skusIds, total_docs
    docs = []
    print("-- generating the write commands -- ")
    print("Reading csv data to generate docs")
    progress = tqdm(unit="docs", total=doc_limit)
    while total_docs < doc_limit:
        with open(input_data_filename, newline='') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',')
            for row in spamreader:
                nodes, total_nodes, docs_map, added_docs, skusIds = process_inventory(row, 5, nodes, total_nodes,
                                                                                      docs_map, skusIds,
                                                                                      countries_alpha_3,
                                                                                      countries_alpha_p)
                total_docs = total_docs + added_docs
                if total_docs > doc_limit:
                    break
                progress.update(added_docs)
        if total_docs > doc_limit:
            break
    progress.close()
    total_skids = len(list(skusIds.keys()))
    print("Generated {} total docs with {} distinct skids and {} distinct nodes".format(total_docs, total_skids,
                                                                                        total_nodes))


def save_setup_csv_command_list():
    global all_csvfile, all_csv_writer, progress, doc, generated_row
    all_csvfile = open(all_fname, 'w', newline='')
    setup_csvfile = open(setup_fname, 'w', newline='')
    all_csv_writer = csv.writer(all_csvfile, delimiter=',')
    setup_csv_writer = csv.writer(setup_csvfile, delimiter=',')
    progress = tqdm(unit="docs", total=total_docs)
    for doc in docs_map.values():
        generated_row = generate_ft_add_row(indexname, doc)
        all_csv_writer.writerow(generated_row)
        setup_csv_writer.writerow(generated_row)
        progress.update()
    progress.close()
    setup_csvfile.close()


def generate_benchmark_commands():
    global all_csvfile, progress, doc, generated_row, total_updates, total_reads
    print("-- generating {} update/read commands -- ".format(total_benchmark_commands))
    print("\t saving to {} and {}".format(bench_fname, all_fname))
    all_csvfile = open(all_fname, 'a', newline='')
    bench_csvfile = open(bench_fname, 'w', newline='')
    bench_csv_writer = csv.writer(bench_csvfile, delimiter=',')
    docs_list = list(docs_map.values())
    skusIds_list = list(skusIds.keys())
    nodesIds = ["{}".format(x) for x in range(1, total_nodes)]
    csv_writer = csv.writer(csvfile, delimiter=',')
    progress = tqdm(unit="docs", total=total_benchmark_commands)
    for _ in range(0, total_benchmark_commands):
        choice = random.choices(["update", "read"], weights=[update_ratio, read_ratio])[0]
        if choice == "update":
            random_doc_pos = random.randint(0, total_docs - 1)
            doc = docs_list[random_doc_pos]
            generated_row = generate_ft_add_update_row(indexname, doc)
            total_updates = total_updates + 1
        elif choice == "read":
            generated_row = generate_ft_aggregate_row(indexname, countries_alpha_3, countries_alpha_p,
                                                      max_skus_per_aggregate, skusIds_list,
                                                      max_nodes_per_aggregate, nodesIds)
            total_reads = total_reads + 1
        all_csv_writer.writerow(generated_row)
        bench_csv_writer.writerow(generated_row)
        progress.update()
    progress.close()
    bench_csvfile.close()
    all_csvfile.close()


""" Returns a human readable string reprentation of bytes"""


def humanized_bytes(bytes, units=[' bytes', 'KB', 'MB', 'GB', 'TB']):
    return str(bytes) + " " + units[0] if bytes < 1024 else humanized_bytes(bytes >> 10, units[1:])


def generate_inputs_dict_item(type, all_fname, description, remote_url, uncompressed_size, compressed_filename,
                              compressed_size, total_commands, command_category):
    dict = {
        "local-uncompressed-filename": all_fname,
        "local-compressed-filename": compressed_filename,
        "type": type,
        "description": description,
        "remote-url": remote_url,
        "compressed-bytes": compressed_size,
        "compressed-bytes-humanized": humanized_bytes(compressed_size),
        "uncompressed-bytes": uncompressed_size,
        "uncompressed-bytes-humanized": humanized_bytes(uncompressed_size),
        "total-commands": total_commands,
        "command-category": command_category,
    }
    return dict


if (__name__ == "__main__"):
    parser = argparse.ArgumentParser(description='RediSearch FTSB data generator.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--update-ratio', type=float, default=0.85, )
    parser.add_argument('--seed', type=int, default=12345, )
    parser.add_argument('--doc-limit', type=int, default=1000000, )
    parser.add_argument('--total-benchmark-commands', type=int, default=1000000, )
    parser.add_argument('--max-skus-per-aggregate', type=int, default=100, )
    parser.add_argument('--max-nodes-per-aggregate', type=int, default=100, )
    parser.add_argument('--indexname', type=str, default="inventory", )
    parser.add_argument('--test-name', type=str, default="ecommerce-inventory", )
    parser.add_argument('--test-description', type=str,
                        default="benchmark focused on updates and aggregate performance", )
    parser.add_argument('--countries-alpha3', type=str, default="US,CA,FR,IL,UK")
    parser.add_argument('--countries-alpha3-probability', type=str, default="0.8,0.05,0.05,0.05,0.05")
    parser.add_argument('--benchmark-output-file-prefix', type=str, default="ecommerce-inventory.redisearch.commands", )
    parser.add_argument('--benchmark-config-file', type=str, default="ecommerce-inventory.redisearch.cfg.json", )
    parser.add_argument('--upload-artifacts-s3', default=False, action='store_true',
                        help="uploads the generated dataset files and configuration file to public benchmarks.redislabs bucket. Proper credentials are required")
    parser.add_argument('--input-data-filename', type=str,
                        default="./../../scripts/usecases/ecommerce/amazon_co-ecommerce_sample.csv", )
    args = parser.parse_args()
    use_case_specific_arguments = dict(args.__dict__)
    del use_case_specific_arguments["upload_artifacts_s3"]
    del use_case_specific_arguments["test_name"]
    del use_case_specific_arguments["test_description"]
    del use_case_specific_arguments["benchmark_config_file"]
    del use_case_specific_arguments["benchmark_output_file_prefix"]
    print(use_case_specific_arguments)
    seed = args.seed
    update_ratio = args.update_ratio
    read_ratio = 1 - update_ratio
    doc_limit = args.doc_limit
    total_benchmark_commands = args.total_benchmark_commands
    max_skus_per_aggregate = args.max_skus_per_aggregate
    max_nodes_per_aggregate = args.max_nodes_per_aggregate
    indexname = args.indexname
    input_data_filename = args.input_data_filename
    benchmark_output_file = args.benchmark_output_file_prefix
    benchmark_config_file = args.benchmark_config_file
    used_indices = [indexname]
    setup_commands = []
    teardown_commands = []
    total_writes = 0
    total_reads = 0
    total_updates = 0
    total_deletes = 0
    description = args.test_description
    test_name = args.test_name
    s3_bucket_name = "benchmarks.redislabs"
    s3_bucket_path = "redisearch/datasets/{}/".format(test_name)
    s3_uri = "https://s3.amazonaws.com/{bucket_name}/{bucket_path}".format(bucket_name=s3_bucket_name,
                                                                           bucket_path=s3_bucket_path)
    all_fname = "{}.ALL.csv".format(benchmark_output_file)
    setup_fname = "{}.SETUP.csv".format(benchmark_output_file)
    bench_fname = "{}.BENCH.csv".format(benchmark_output_file)
    all_fname_compressed = "{}.ALL.tar.gz".format(benchmark_output_file)
    setup_fname_compressed = "{}.SETUP.tar.gz".format(benchmark_output_file)
    bench_fname_compressed = "{}.BENCH.tar.gz".format(benchmark_output_file)
    remote_url_all = "{}{}".format(s3_uri, all_fname_compressed)
    remote_url_setup = "{}{}".format(s3_uri, setup_fname_compressed)
    remote_url_bench = "{}{}".format(s3_uri, bench_fname_compressed)
    json_version = "0.1"
    benchmark_repetitions_require_teardown_and_resetup = False

    print("-- Benchmark: {} -- ".format(description))
    print("-- Description: {} -- ".format(description))

    countries_alpha_3 = args.countries_alpha3.split(",")
    countries_alpha_p = [float(x) for x in args.countries_alpha3_probability.split(",")]
    docs_map = {}
    nodes = {}
    skusIds = {}
    total_nodes = 0
    total_docs = 0

    countries_p_str = []
    for idx, country in enumerate(countries_alpha_3):
        countries_p_str.append("{} {}%".format(country, countries_alpha_p[idx] * 100.0))
    print("Using {0} countries with the following probabilities {1}".format(len(countries_alpha_3),
                                                                            " ".join(countries_p_str)))
    print("Using random seed {0}".format(args.seed))
    random.seed(args.seed)

    generate_setup_commands()
    print("\t saving to {} and {}".format(setup_fname, all_fname))
    save_setup_csv_command_list()

    print("-- generating the ft.create commands -- ")
    ft_create_cmd = generate_ft_create_row(indexname, list(docs_map.values())[0])
    print(" ".join(ft_create_cmd))
    setup_commands.append(ft_create_cmd)

    generate_benchmark_commands()
    total_setup_commands = total_docs
    total_commands = total_setup_commands + total_benchmark_commands
    cmd_category_all = {
        "setup-writes": total_docs,
        "writes": total_writes,
        "updates": total_updates,
        "reads": total_reads,
        "deletes": total_deletes,
    }
    cmd_category_setup = {
        "setup-writes": total_docs,
        "writes": 0,
        "updates": 0,
        "reads": 0,
        "deletes": 0,
    }
    cmd_category_benchmark = {
        "setup-writes": 0,
        "writes": total_writes,
        "updates": total_updates,
        "reads": total_reads,
        "deletes": total_deletes,
    }

    status, uncompressed_size, compressed_size = compress_files([all_fname], all_fname_compressed)
    inputs_entry_all = generate_inputs_dict_item("all", all_fname, "contains both setup and benchmark commands",
                                                 remote_url_all, uncompressed_size, all_fname_compressed,
                                                 compressed_size, total_commands, cmd_category_all)

    status, uncompressed_size, compressed_size = compress_files([setup_fname], setup_fname_compressed)
    inputs_entry_setup = generate_inputs_dict_item("setup", setup_fname,
                                                   "contains only the commands required to populate the dataset",
                                                   remote_url_setup, uncompressed_size, setup_fname_compressed,
                                                   compressed_size, total_setup_commands, cmd_category_setup)

    status, uncompressed_size, compressed_size = compress_files([bench_fname], bench_fname_compressed)
    inputs_entry_benchmark = generate_inputs_dict_item("benchmark", bench_fname,
                                                       "contains only the benchmark commands (required the dataset to have been previously populated)",
                                                       remote_url_bench, uncompressed_size, bench_fname_compressed,
                                                       compressed_size, total_benchmark_commands,
                                                       cmd_category_benchmark)

    inputs = {"all": inputs_entry_all, "setup": inputs_entry_setup, "benchmark": inputs_entry_benchmark}

    with open(benchmark_config_file, "w") as setupf:
        setup_json = generate_setup_json(json_version, use_case_specific_arguments, test_name, description, inputs,
                                         setup_commands,
                                         teardown_commands,
                                         used_indices,
                                         total_commands,
                                         total_setup_commands,
                                         total_benchmark_commands, total_docs, total_writes, total_updates, total_reads,
                                         total_deletes,
                                         benchmark_repetitions_require_teardown_and_resetup,
                                         ["setup"],
                                         ["benchmark"]
                                         )
        json.dump(setup_json, setupf, indent=2)

    if args.upload_artifacts_s3:
        print("-- uploading dataset to s3 -- ")
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(s3_bucket_name)
        artifacts = [benchmark_config_file, all_fname_compressed, setup_fname_compressed, bench_fname_compressed]
        progress = tqdm(unit="files", total=len(artifacts))
        for input in artifacts:
            object_key = '{bucket_path}{filename}'.format(bucket_path=s3_bucket_path, filename=input)
            bucket.upload_file(input, object_key)
            object_acl = s3.ObjectAcl(s3_bucket_name, object_key)
            response = object_acl.put(ACL='public-read')
            progress.update()
        progress.close()
