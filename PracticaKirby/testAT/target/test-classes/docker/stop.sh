#!/usr/bin/env bash
#abort on errors
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker-compose -f ${SCRIPT_DIR}/datio/docker-compose.yml down
docker rmi datio_hadoop
docker rmi datio_nginx
docker rmi datio_node
docker rmi datio_master
docker rmi datio_worker
rm -rf ${SCRIPT_DIR}/datio/hadoop/hdfsTestFiles
rm -rf ${SCRIPT_DIR}/datio/nginx/schema