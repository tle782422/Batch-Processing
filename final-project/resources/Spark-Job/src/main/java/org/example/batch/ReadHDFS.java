package org.example.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.common.util.SparkUtils;
import org.example.config.HadoopConfig;

public class ReadHDFS {
    public static void main(String[] args) {

        // Khởi tạo phiên làm việc
        SparkSession spark = SparkSession.builder()
                .appName("Test")
                .master("spark://spark-master:7077")
                .getOrCreate();

        // Cấu hình HDFS
        Configuration hadoopConf = HadoopConfig.configureHadoop();

        // Đọc file trong HDFS
        Dataset<Row> df = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .option("dateFormat", "yyyy-MM-dd")
                .csv("hdfs://namenode:9000/output/flight_data.csv");


        df.printSchema();

        df.show(20);

        spark.stop();

    }
}
