#!/bin/bash

set -e

DEBUG=${DEBUG:-0}
SEED=${SEED:-12345}
MAX_DOCS=${MAX_DOCS:-1000000}
MAX_INSERTS=${MAX_INSERTS:-0}
MAX_QUERIES=${MAX_QUERIES:-100000}
BATCH_SIZE=${BATCH_SIZE:-1}
PIPELINE=${PIPELINE:-100}
UPDATE_RATE=${UPDATE_RATE:-0.0}
REPLACE_PARTIAL=${REPLACE_PARTIAL:-false}
REPLACE_CONDITION=${REPLACE_CONDITION:-""}
DELETE_RATE=${DELETE_RATE:-0.0}
NOSAVE=${NOSAVE:-"false"}
FORMAT=${FORMAT:-"redisearch"}
PRINT_INTERVAL=100000
IP=${IP:-"localhost"}
PORT=${PORT:-6379}
HOST="$IP:$PORT"
USE_HASHES=${USE_HASHES:-"false"}
HAS_PREFIX=${HAS_PREFIX:-"true"}
# How many queries would be run
REPORTING_PERIOD=${REPORTING_PERIOD:-"1s"}
# How many concurrent worker would run queries - match num of cores, or default to 8
WORKERS=${WORKERS:-$(grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 8)}

echo "Using ${WORKERS} WORKERS"
