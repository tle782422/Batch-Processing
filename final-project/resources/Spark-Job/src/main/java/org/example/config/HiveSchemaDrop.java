package org.example.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.common.util.SparkUtils;

import java.net.URI;
import java.util.List;

public class HiveSchemaDrop {

    public static void main(String[] args) {
        SparkSession spark = SparkUtils.SparkSessionInit(
                "Test",
                "hdfs://namenode:9000/user/hive/warehouse",
                "thrift://hive-metastore:9083"
        );


        Configuration hadoopConf = HadoopConfig.configureHadoop();
        try {
            FileSystem hdfs = FileSystem.get(new URI("hdfs://namenode:9000"), hadoopConf);

            String[] directories = {
                    "/user/hive/warehouse/batch-processing-report"
            };

            for (String dir : directories) {
                Path path = new Path(dir);
                if (!hdfs.exists(path)) {
                    hdfs.delete(path);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            // Step 1: Drop all tables in the database
            dropAllTables(spark, "batchreports");

            // Step 2: Drop the database
            spark.sql("DROP DATABASE IF EXISTS batchreports CASCADE");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Stop the SparkSession
            spark.stop();
        }

    }

    private static void dropAllTables(SparkSession spark, String databaseName) throws Exception {
        String getTablesQuery = "SHOW TABLES IN " + databaseName;
        spark.sql("USE " + databaseName);
        List<Row> tables = spark.sql(getTablesQuery).toJavaRDD().collect();

        for (Row row : tables) {
            String tableName = row.getString(1); // Table name is in the second column
            String dropTableQuery = "DROP TABLE IF EXISTS " + databaseName + "." + tableName;
            spark.sql(dropTableQuery);
        }
    }

}
