CREATE DATABASE IF NOT EXISTS batchreports WITH DBPROPERTIES (
    'LOCATION' = 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report'
);

CREATE EXTERNAL TABLE IF NOT EXISTS batchreports.delay_total (
    OP_CARRIER STRING,
    AVG_DEP_DELAY FLOAT,
    AVG_ARR_DELAY FLOAT
) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/delay_total';

CREATE EXTERNAL TABLE IF NOT EXISTS batchreports.delay_year (
    OP_CARRIER STRING,
    AVG_DEP_DELAY FLOAT,
    AVG_ARR_DELAY FLOAT
) PARTITIONED BY (YEAR INT)  LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/delay_year';

CREATE EXTERNAL TABLE IF NOT EXISTS batchreports.delay_year_month (
    OP_CARRIER STRING,
    AVG_DEP_DELAY FLOAT,
    AVG_ARR_DELAY FLOAT
) PARTITIONED BY (YEAR INT, MONTH INT) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/delay_year_month';

CREATE EXTERNAL TABLE IF NOT EXISTS batchreports.delay_dayofweek (
    OP_CARRIER STRING,
    AVG_DEP_DELAY FLOAT,
    AVG_ARR_DELAY FLOAT
) PARTITIONED BY (DAY_OF_WEEK INT) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/delay_dayofweek';

CREATE EXTERNAL TABLE IF NOT EXISTS batchreports.delay_total_src_dest (
    ORIGIN STRING,
    DEST STRING,
    AVG_DEP_DELAY FLOAT,
    AVG_ARR_DELAY FLOAT
) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/delay_total_src_dest';

CREATE EXTERNAL TABLE IF NOT EXISTS batchreports.delay_year_src_dest (
    ORIGIN STRING,
    DEST STRING,
    AVG_DEP_DELAY FLOAT,
    AVG_ARR_DELAY FLOAT
) PARTITIONED BY (YEAR INT) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/delay_year_src_dest';

CREATE EXTERNAL TABLE IF NOT EXISTS batchreports.delay_year_month_src_dest (
    ORIGIN STRING,
    DEST STRING,
    AVG_DEP_DELAY FLOAT,
    AVG_ARR_DELAY FLOAT
) PARTITIONED BY (YEAR INT, MONTH INT) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/delay_year_month_src_dest';

CREATE EXTERNAL TABLE IF NOT EXISTS batchreports.delay_dayofweek_src_dest (
    ORIGIN STRING,
    DEST STRING,
    AVG_DEP_DELAY FLOAT,
    AVG_ARR_DELAY FLOAT
) PARTITIONED BY (DAY_OF_WEEK INT) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/delay_dayofweek_src_dest';

CREATE EXTERNAL TABLE IF NOT EXISTS batchreports.cancellation_diverted_total_src_dest (
    ORIGIN STRING,
    DEST STRING,
    CANC_PERC FLOAT,
    CANC_COUNT BIGINT,
    DIV_PERC FLOAT,
    DIV_COUNT BIGINT
) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/cancellation_diverted_total_src_dest';

CREATE EXTERNAL TABLE IF NOT EXISTS batchreports.cancellation_diverted_year_src_dest (
    ORIGIN STRING,
    DEST STRING,
    CANC_PERC FLOAT,
    CANC_COUNT BIGINT,
    DIV_PERC FLOAT,
    DIV_COUNT BIGINT
) PARTITIONED BY (YEAR INT) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/cancellation_diverted_year_src_dest';

CREATE EXTERNAL TABLE IF NOT EXISTS batchreports.cancellation_diverted_year_month_src_dest (
    ORIGIN STRING,
    DEST STRING,
    CANC_PERC FLOAT,
    CANC_COUNT BIGINT,
    DIV_PERC FLOAT,
    DIV_COUNT BIGINT
) PARTITIONED BY (YEAR INT, MONTH INT) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/cancellation_diverted_year_month_src_dest';

CREATE EXTERNAL TABLE IF NOT EXISTS batchreports.cancellation_diverted_dayofweek_src_dest (
    ORIGIN STRING,
    DEST STRING,
    CANC_PERC FLOAT,
    CANC_COUNT BIGINT,
    DIV_PERC FLOAT,
    DIV_COUNT BIGINT
) PARTITIONED BY (DAY_OF_WEEK INT) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/cancellation_diverted_dayofweek_src_dest';

CREATE EXTERNAL TABLE IF NOT EXISTS batchreports.cancellation_diverted_total (
    OP_CARRIER STRING,
    CANC_PERC FLOAT,
    CANC_COUNT BIGINT,
    DIV_PERC FLOAT,
    DIV_COUNT BIGINT
) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/cancellation_diverted_total';

CREATE EXTERNAL TABLE IF NOT EXISTS batchreports.cancellation_diverted_year (
    OP_CARRIER STRING,
    CANC_PERC FLOAT,
    CANC_COUNT BIGINT,
    DIV_PERC FLOAT,
    DIV_COUNT BIGINT
) PARTITIONED BY (YEAR INT) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/cancellation_diverted_year';

CREATE TABLE IF NOT EXISTS batchreports.cancellation_diverted_year_month (
    OP_CARRIER STRING,
    CANC_PERC FLOAT,
    CANC_COUNT BIGINT,
    DIV_PERC FLOAT,
    DIV_COUNT BIGINT
) PARTITIONED BY (YEAR INT, MONTH INT) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/cancellation_diverted_year_month';

CREATE TABLE IF NOT EXISTS batchreports.cancellation_diverted_dayofweek (
    OP_CARRIER STRING,
    CANC_PERC FLOAT,
    CANC_COUNT BIGINT,
    DIV_PERC FLOAT,
    DIV_COUNT BIGINT
) PARTITIONED BY (DAY_OF_WEEK INT) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/cancellation_diverted_dayofweek';

CREATE TABLE IF NOT EXISTS batchreports.dist_total(
    OP_CARRIER STRING,
    TOTAL_DISTANCE FLOAT
) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/dist_total';

CREATE TABLE IF NOT EXISTS batchreports.dist_year(
    OP_CARRIER STRING,
    TOTAL_DISTANCE FLOAT
) PARTITIONED BY (YEAR INT) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/dist_year';

CREATE TABLE IF NOT EXISTS batchreports.dist_year_month(
    OP_CARRIER STRING,
    TOTAL_DISTANCE FLOAT
) PARTITIONED BY (YEAR INT, MONTH INT) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/dist_year_month';

CREATE TABLE IF NOT EXISTS batchreports.dist_dayofweek (
    OP_CARRIER STRING,
    TOTAL_DISTANCE FLOAT
) PARTITIONED BY (DAY_OF_WEEK INT) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/dist_dayofweek';

CREATE TABLE IF NOT EXISTS batchreports.max_consec_delay_year (
    OP_CARRIER STRING,
    MAX_GIORNI BIGINT
) PARTITIONED BY (YEAR INT) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/max_consec_delay_year';

CREATE TABLE IF NOT EXISTS batchreports.max_consec_delay_year_src_dest(
    ORIGIN STRING,
    DEST STRING,
    MAX_GIORNI BIGINT
) PARTITIONED BY (YEAR INT) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/max_consec_delay_year_src_dest';

CREATE TABLE IF NOT EXISTS batchreports.src_dest_canc_code (
    ORIGIN STRING,
    DEST STRING,
    CANCELLATION_CODE STRING,
    NUM_CANCELLED BIGINT
) PARTITIONED BY (YEAR INT) LOCATION 'hdfs://namenode:9000/user/hive/warehouse/batch-processing-report/src_dest_canc_code';

