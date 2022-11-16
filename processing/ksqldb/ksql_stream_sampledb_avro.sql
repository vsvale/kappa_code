-- kubectl get pods -n processing
-- KSQLDB=nomedopod
-- kubectl exec $KSQLDB -n processing -ti -- bash ksql
SET 'auto.offset.reset' = 'earliest';
SHOW TOPICS;
SHOW STREAMS;
CREATE OR REPLACE STREAM ksql_stream_sampledb_customer_avro WITH (KAFKA_TOPIC='src-sqlserver-jdbc-sampledb-custumer-avro', VALUE_FORMAT='AVRO', KEY_FORMAT='AVRO');
CREATE OR REPLACE STREAM ksql_stream_sampledb_salesorderheader_avro WITH (KAFKA_TOPIC='src-sqlserver-jdbc-sampledb-salesorderheader-avro', VALUE_FORMAT='AVRO', KEY_FORMAT='AVRO');
select * from ksql_stream_sampledb_customer_avro emit changes limit 1;
select * from ksql_stream_sampledb_salesorderheader_avro emit changes limit 1;
SELECT SalesPerson, count(SalesPerson) as "number_clients" from ksql_stream_sampledb_customer_avro WINDOW SESSION (5 SECONDS) group by SalesPerson EMIT CHANGES;

CREATE OR REPLACE STREAM output_ksql_stream_sampledb_customerdates_avro
WITH (KAFKA_TOPIC='output_ksql_stream_sampledb_customerdates_avro',PARTITIONS=6, VALUE_FORMAT='AVRO')
AS
SELECT
order.CUSTOMERID,
order.PURCHASEORDERNUMBER,
order.ORDERDATE,
order.SHIPDATE,
order.DUEDATE,
CONCAT_WS(' ',CAST(client.FIRSTNAME AS VARCHAR),CAST(client.MIDDLENAME AS VARCHAR),CAST(client.LASTNAME AS VARCHAR)) AS CUSTOMER,
MASK(client.EMAILADDRESS),
TIMESTAMPTOSTRING(order.ROWTIME,'yyyy-MM-dd HH:mm:ss', 'America/Sao_Paulo') AS "processing_time"
FROM ksql_stream_sampledb_salesorderheader_avro AS order
INNER JOIN ksql_stream_sampledb_customer_avro AS client WITHIN 36500 DAYS
ON order.CUSTOMERID = client.CUSTOMERID
PARTITION BY order.CUSTOMERID
EMIT CHANGES;

SELECT * FROM output_ksql_stream_sampledb_customerdates_avro emit changes limit 5;

CREATE OR REPLACE TABLE output_ksql_tb_sampledb_customerdates_avro
WITH (KAFKA_TOPIC='output_ksql_tb_sampledb_customerdates_avro', PARTITIONS=6, VALUE_FORMAT='AVRO')
AS
SELECT
CUSTOMER,
COUNT(CUSTOMER) AS "QTY_ORDERS"
FROM output_ksql_stream_sampledb_customerdates_avro WINDOW TUMBLING (SIZE 36500 DAYS)
GROUP BY CUSTOMER
EMIT CHANGES;

SELECT * FROM output_ksql_tb_sampledb_customerdates_avro emit changes limit 5;