CREATE DATABASE IF NOT EXISTS ods;

CREATE EXTERNAL TABLE IF NOT EXISTS ods.orders_hbase (
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
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" = ":key,info:user_id,info:product_id,info:category,info:price,info:quantity,info:amount,info:order_time,info:status"
)
TBLPROPERTIES (
  "hbase.table.name" = "orders",
  "hbase.mapred.output.outputtable" = "orders"
);
