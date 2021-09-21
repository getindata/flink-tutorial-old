#!/bin/bash

sed -i "s/jobmanager.rpc.address: localhost/jobmanager.rpc.address: ${JOB_MANAGER_RPC_ADDRESS}/" /opt/flink/conf/flink-conf.yaml
tail -f /dev/null
