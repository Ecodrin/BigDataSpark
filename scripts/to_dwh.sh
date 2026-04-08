#!/bin/bash
docker exec spark /opt/spark/bin/spark-submit --master local[*] \
        --jars /opt/etl/drivers/postgresql-42.7.10.jar \
        /opt/etl/scripts/to_dwh.py
echo "to dwh succes"