package org.example.ingestion;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.example.config.StructConfigure;
import org.example.common.util.SparkUtils;
import org.example.config.HadoopConfig;


public class SparkPreprocessing {

    // Trong bài chỉ thực hiện với 1.4GB tương ứng 2009 và 2010 vì máy không đủ mạnh để thực hiện trên 8GB dữ liệu
    // Thao tác Transforms sẽ được thực hiện với final.csv
    public static void main(String[] args) {
        SparkSession spark = SparkUtils.SparkSessionInit(
        "Test",
        "hdfs://namenode:9000/user/hive/warehouse",
        "thrift://hive-metastore:9083"
        );

        Configuration hadoopConf = HadoopConfig.configureHadoop();

        StructType schema = StructConfigure.SparkStruct();

        Dataset<Row> df_2009 = spark.read().option("header", true).schema(schema).csv("hdfs://namenode:9000/input/2009.csv");
        Dataset<Row> df_2010 = spark.read().option("header", true).schema(schema).csv("hdfs://namenode:9000/input/2010.csv");

        df_2009 = df_2009.drop("Unnamed: 27");
        df_2010 = df_2010.drop("Unnamed: 27");

        Dataset<Row> df = df_2009
                        .union(df_2010)
                        .dropDuplicates();

        df.coalesce(1)
                .write()
                .mode("overwrite")
                .option("header", "true")
                .parquet("hdfs://namenode:9000/output/flight_data.parquet");

        spark.stop();

    }
}



