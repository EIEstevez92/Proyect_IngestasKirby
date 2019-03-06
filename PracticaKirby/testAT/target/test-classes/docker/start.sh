#!/usr/bin/env bash
#abort on errors
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ ! -d ${SCRIPT_DIR}/datio/spark/kirby ]; then
    mkdir -p $SCRIPT_DIR/datio/spark/kirby
fi

for z in ${SCRIPT_DIR}/datio/spark/kirby/co*; do
        rm -f "$z" || continue
        echo "Removing $z lib..."
done

find ${SCRIPT_DIR}/../../../../ingestion/target/kirby-ingestion-*-jar-with-dependencies.jar -exec cp {} ${SCRIPT_DIR}/datio/spark/kirby/ \;
find ${SCRIPT_DIR}/../../../../api-examples/target/kirby-api-examples-*-jar-with-dependencies.jar -exec cp {} ${SCRIPT_DIR}/datio/spark/kirby/ \;


cp -rf ${SCRIPT_DIR}/../hdfsTestFiles/ ${SCRIPT_DIR}/datio/hadoop/
cp -rf ${SCRIPT_DIR}/../hdfsTestFiles/tests/flow/schema ${SCRIPT_DIR}/datio/nginx/

docker-compose -f ${SCRIPT_DIR}/datio/docker-compose.yml rm
docker-compose -f ${SCRIPT_DIR}/datio/docker-compose.yml up -d

#i=0
while ! $(docker-compose -f ${SCRIPT_DIR}/datio/docker-compose.yml logs hadoop | grep -lq COPIED)
do
#    echo "ESPERO $i"
#    i=$(( $i + 3 ))
    sleep 3
done