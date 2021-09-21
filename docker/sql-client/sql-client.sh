#!/bin/bash

# shellcheck disable=SC2086
"${FLINK_HOME}"/bin/sql-client.sh embedded -d "${SQL_CLIENT_HOME}"/conf/sql-client-conf.yaml -l ${SQL_CLIENT_HOME}/lib
