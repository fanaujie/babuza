#!/bin/bash

STORE_ID=$1
SHARDS=${2:-32}
DATA_DIR=${3:-"./data$STORE_ID"}
REDIS_PORT=$((10000 + $STORE_ID))
RAFT_PORT=$((14199 + $STORE_ID))

echo "Waiting for 3 seconds before starting redis-cluster with store-id $STORE_ID..."
sleep 3

echo "Starting redis-cluster with store-id $STORE_ID..."
./redis-cluster server \
    --shards $SHARDS \
    --store-id $STORE_ID \
    --data-dir $DATA_DIR \
    --redis-address 127.0.0.1:$REDIS_PORT \
    --raft-address 127.0.0.1:$RAFT_PORT \
    --initial-raft-stores 1=127.0.0.1:14200,2=127.0.0.1:14201,3=127.0.0.1:14202
