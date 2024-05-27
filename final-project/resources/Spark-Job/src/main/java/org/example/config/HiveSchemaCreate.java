package org.example.config;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.spark.sql.SparkSession;
import org.example.common.util.SparkUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;


public class HiveSchemaCreate {

    public static void main(String[] args) {

        SparkSession spark = SparkUtils.SparkSessionInit(
                "Test",
                "hdfs://namenode:9000/user/hive/warehouse",
                "thrift://hive-metastore:9083"
        );



        // Đọc file hiveschema.hql từ resources
        InputStream inputStream = HiveSchemaCreate.class.getResourceAsStream("/hive-schema.hql");

        if (inputStream == null) {
            System.out.println("File not found!");
            System.exit(1);
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            StringBuilder hqlScript = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                hqlScript.append(line).append("\n");
            }

            // Chia nội dung file thành các câu lệnh SQL
            String[] sqlStatements = hqlScript.toString().split(";");

            // Thực thi từng câu lệnh SQL
            for (String sql : sqlStatements) {
                String trimmedSql = sql.trim();
                if (!trimmedSql.isEmpty()) {
                    System.out.println("Executing SQL: " + trimmedSql);
                    spark.sql(trimmedSql);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        Configuration hadoopConf = HadoopConfig.configureHadoop();
        try {
            FileSystem hdfs = FileSystem.get(new URI("hdfs://namenode:9000"), hadoopConf);

            // Directories to be created
            String[] directories = {
                    "/user/hive/warehouse/batch-processing-report/delay_total",
                    "/user/hive/warehouse/batch-processing-report/delay_year",
                    "/user/hive/warehouse/batch-processing-report/delay_year_month",
                    "/user/hive/warehouse/batch-processing-report/delay_dayofweek",
                    "/user/hive/warehouse/batch-processing-report/delay_total_src_dest",
                    "/user/hive/warehouse/batch-processing-report/delay_year_src_dest",
                    "/user/hive/warehouse/batch-processing-report/delay_year_month_src_dest",
                    "/user/hive/warehouse/batch-processing-report/delay_dayofweek_src_dest",
                    "/user/hive/warehouse/batch-processing-report/cancellation_diverted_total_src_dest",
                    "/user/hive/warehouse/batch-processing-report/cancellation_diverted_year_src_dest",
                    "/user/hive/warehouse/batch-processing-report/cancellation_diverted_year_month_src_dest",
                    "/user/hive/warehouse/batch-processing-report/cancellation_diverted_dayofweek_src_dest",
                    "/user/hive/warehouse/batch-processing-report/cancellation_diverted_total",
                    "/user/hive/warehouse/batch-processing-report/cancellation_diverted_year",
                    "/user/hive/warehouse/batch-processing-report/cancellation_diverted_year_month",
                    "/user/hive/warehouse/batch-processing-report/cancellation_diverted_dayofweek",
                    "/user/hive/warehouse/batch-processing-report/dist_total",
                    "/user/hive/warehouse/batch-processing-report/dist_year",
                    "/user/hive/warehouse/batch-processing-report/dist_year_month",
                    "/user/hive/warehouse/batch-processing-report/dist_dayofweek",
                    "/user/hive/warehouse/batch-processing-report/max_consec_delay_year",
                    "/user/hive/warehouse/batch-processing-report/max_consec_delay_year_src_dest",
                    "/user/hive/warehouse/batch-processing-report/src_dest_canc_code"
            };

            // Create directories
            for (String dir : directories) {
                Path path = new Path(dir);
                if (!hdfs.exists(path)) {
                    hdfs.mkdirs(path);
                    hdfs.setPermission(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Dừng SparkSession
        spark.stop();
    }
}