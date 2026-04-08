#!/bin/bash


docker exec spark /opt/spark/bin/spark-submit --master local[*] \
        --jars /opt/etl/drivers/mongo-spark-connector_2.13-11.0.1.jar,/opt/etl/drivers/mongodb-driver-core-5.6.4.jar,/opt/etl/drivers/bson-5.6.4.jar,/opt/etl/drivers/mongodb-driver-sync-5.6.4.jar,/opt/etl/drivers/postgresql-42.7.10.jar \
        /opt/etl/scripts/to_mongo.py

echo "to mongo success"