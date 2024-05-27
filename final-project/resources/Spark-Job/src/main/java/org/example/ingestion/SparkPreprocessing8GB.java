package org.example.ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.example.config.StructConfigure;
import org.example.common.util.SparkUtils;


public class SparkPreprocessing8GB {

    public static void main(String[] args) {

        SparkSession spark = SparkUtils.SparkSessionInit(
                "Test",
                "hdfs://namenode:9000/user/hive/warehouse",
                "thrift://hive-metastore:9083"
        );

        StructType schema = StructConfigure.SparkStruct();

        //https://www.kaggle.com/datasets/sherrytp/airline-delay-analysis?resource=download
        Dataset<Row> df_2009 = spark.read().option("header", true).schema(schema).csv("hdfs://namenode:9000/input/2009.csv");
        Dataset<Row> df_2010 = spark.read().option("header", true).schema(schema).csv("hdfs://namenode:9000/input/2010.csv");
        Dataset<Row> df_2011 = spark.read().option("header", true).schema(schema).csv("hdfs://namenode:9000/input/2011.csv");
        Dataset<Row> df_2012 = spark.read().option("header", true).schema(schema).csv("hdfs://namenode:9000/input/2012.csv");
        Dataset<Row> df_2013 = spark.read().option("header", true).schema(schema).csv("hdfs://namenode:9000/input/2013.csv");
        Dataset<Row> df_2014 = spark.read().option("header", true).schema(schema).csv("hdfs://namenode:9000/input/2014.csv");
        Dataset<Row> df_2015 = spark.read().option("header", true).schema(schema).csv("hdfs://namenode:9000/input/2015.csv");
        Dataset<Row> df_2016 = spark.read().option("header", true).schema(schema).csv("hdfs://namenode:9000/input/2016.csv");
        Dataset<Row> df_2017 = spark.read().option("header", true).schema(schema).csv("hdfs://namenode:9000/input/2017.csv");
        Dataset<Row> df_2018 = spark.read().option("header", true).schema(schema).csv("hdfs://namenode:9000/input/2018.csv");

        df_2009 = df_2009.drop("Unnamed: 27");
        df_2010 = df_2010.drop("Unnamed: 27");
        df_2011 = df_2011.drop("Unnamed: 27");
        df_2012 = df_2012.drop("Unnamed: 27");
        df_2013 = df_2013.drop("Unnamed: 27");
        df_2014 = df_2014.drop("Unnamed: 27");
        df_2015 = df_2015.drop("Unnamed: 27");
        df_2016 = df_2016.drop("Unnamed: 27");
        df_2017 = df_2017.drop("Unnamed: 27");
        df_2018 = df_2018.drop("Unnamed: 27");


        Dataset<Row> df = df_2009
                .union(df_2010)
                .unionByName(df_2011)
                .unionByName(df_2012)
                .unionByName(df_2013)
                .unionByName(df_2014)
                .unionByName(df_2015)
                .unionByName(df_2016)
                .unionByName(df_2017)
                .unionByName(df_2018)
                .dropDuplicates();


        df.coalesce(1)
            .write()
            .mode("overwrite")
            .option("header", "true")
            .parquet("hdfs://namenode:9000/output/flight_data.parquet");


        spark.stop();
    }
}
