#!/bin/bash

docker exec spark /opt/spark/bin/spark-submit --master local[*] \
        --jars /opt/etl/drivers/clickhouse-jdbc-0.9.8-all-dependencies.jar,/opt/etl/drivers/postgresql-42.7.10.jar \
        /opt/etl/scripts/to_clickhouse.py

echo "to clickhouse success"