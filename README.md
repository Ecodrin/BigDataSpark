# BigDataSpark

Инструкция:
1. ```git clone https://github.com/Ecodrin/BigDataSpark.git```
2. ```docker compose up -d```
3. Чтобы запустить ETL контейнер, запустите ```./scripts/run_etl.sh```
    Можно запустить этапы отдельно, для этого запускайте ```./scripts/to_dwh.sh```,  ```./scripts/to_clickshouse.sh```, ```./scripts/to_mongo.sh```
4. Параметры подключения DBeaver: 
    
    a. postgres: user: ```postgres```, password: ```123```, БД: ```postgres```, host: ```localhost```,port: ```5432``` 
    b. clickhouse: user: ```default```, password: ```123```, БД: ```reports```, host: ```localhost```,port: ```8123```
5. Чтобы посмотреть результаты работы в MongoDB(в DBeaver требуется PRO версия), перейдите на ```localhost:8081```.


Отчеты:
1. ```reports.products``` --- ТОП продаваемых продуктов.
2. ```reports.customers``` --- средний чек для каждого клиента.
3. ```reports.month_sale``` --- средний размер заказа по месяцем(сумма и количество).
4. ```reports.store``` --- средний чек для каждого магазина.
5. ```reports.country_supplier``` --- распределение продаж по странам поставщиков.
6. ```reports.product_reviews``` --- ТОП продуктов с наибольшим количеством отзывов.

```
|   docker-compose.yml
│   README.md
│   
├───jars
│       bson-5.6.4.jar
│       clickhouse-jdbc-0.9.8-all-dependencies.jar
│       mongo-spark-connector_2.13-11.0.1.jar
│       mongodb-driver-core-5.6.4.jar
│       mongodb-driver-sync-5.6.4.jar
│       postgresql-42.7.10.jar
│       
├───scripts
│       reports.py
│       run_etl.sh
│       to_clickhouse.py
│       to_clickhouse.sh
│       to_dwh.py
│       to_dwh.sh
│       to_mongo.py
│       to_mongo.sh
│       
├───sql_init_scripts
│       ddl_clickhouse.sql
│       ddl_postgres.sql
│       
└───src
        MOCK_DATA (1).csv
        MOCK_DATA (2).csv
        MOCK_DATA (3).csv
        MOCK_DATA (4).csv
        MOCK_DATA (5).csv
        MOCK_DATA (6).csv
        MOCK_DATA (7).csv
        MOCK_DATA (8).csv
        MOCK_DATA (9).csv
        MOCK_DATA.csv
```



![Лабораторная работа №2](https://github.com/user-attachments/assets/2b854382-4c36-4542-a7fb-04fe82a6f6fa)

