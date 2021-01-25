--Creates a external hive table on existing Hbase table orders.
CREATE EXTERNAL TABLE hif.orders(
rowkey string,
`order_id` string,
`order_date` string,
`cust_id` string,
`order_status` string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,cf:order_id,cf:order_date,cf:cust_id,cf:order_status')
TBLPROPERTIES ("hbase.table.name" = "hif:orders_load");