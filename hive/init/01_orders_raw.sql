CREATE DATABASE IF NOT EXISTS ods;

CREATE EXTERNAL TABLE IF NOT EXISTS ods.orders_raw (
  order_id   STRING,
  user_id    STRING,
  product_id STRING,
  category   STRING,
  price      DOUBLE,
  quantity   INT,
  amount     DOUBLE,
  order_time STRING,
  status     STRING
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/orders';
