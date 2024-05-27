package org.example.common.util;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class SparkUtils {

    /**
     * Khởi tạo phiên làm việc của Spark.
     *
     * @param appName Tên của ứng dụng Spark
     * @param warehouseDir Đường dẫn đến thư mục lưu trữ dữ liệu
     * @param metastoreUri URI của Hive Metastore
     * @return Đối tượng SparkSession đã được khởi tạo
     */
    public static SparkSession SparkSessionInit(
            String appName,
            String warehouseDir,
            String metastoreUri
    ) {
        // Tạo SparkSession với các cấu hình cần thiết
        SparkSession spark = SparkSession.builder()
                .appName(appName)
                .config("spark.sql.warehouse.dir", warehouseDir)
                .config("hive.metastore.uris", metastoreUri)
                .config("hive.exec.dynamic.partition", "true")
                .config("hive.exec.dynamic.partition.mode", "nonstrict")
//                .config("spark.executor.memory", "3g") // 3GB cho mỗi executor
                .config("spark.executor.cores", "1") // 1 cores cho mỗi executor
                .config("spark.executor.instances", "2") // Tổng số executor = 2 workers * 2 executor mỗi worker
//                .config("spark.driver.memory", "2g") // 2GB cho driver

                .enableHiveSupport()
                .getOrCreate();

        // Kiểm tra xem SparkSession có được khởi tạo thành công hay không
        if (spark == null) {
            throw new RuntimeException("SparkSession initialization failed");
        }

        // Đăng nhập thông tin về SparkSession đã tạo
        System.out.println("SparkSession initialized with app name: " + appName);
        System.out.println("Warehouse directory: " + warehouseDir);
        System.out.println("Metastore URI: " + metastoreUri);

        return spark;
    }

    /**
     * Ghi dữ liệu vào Cassandra.
     *
     * @param df DataFrame cần ghi
     * @param keyspace Tên keyspace của Cassandra
     * @param table Tên bảng của Cassandra
     */
    public static void writeToCassandra(Dataset<Row> df, String keyspace, String table) {
        df.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", keyspace)
                .option("table", table)
                .mode(SaveMode.Append)
                .save();
    }

    /**
     * Ghi dữ liệu vào Hive với các phân vùng tùy chọn.
     *
     * @param df DataFrame cần ghi
     * @param table Tên bảng của Hive
     * @param partitions Danh sách các cột phân vùng
     */
    public static void writeToHive(Dataset<Row> df, String table, List<String> partitions) {
        // Kiểm tra xem danh sách phân vùng có rỗng không
        if (partitions == null || partitions.isEmpty()) {
            df.write()
                    .format("hive")
                    .mode(SaveMode.Append)
                    .saveAsTable(table);
        } else {
            // Tạo mảng String từ danh sách phân vùng
            String[] partitionArray = partitions.toArray(new String[0]);

            df.write()
                    .format("hive")
                    .partitionBy(partitionArray)
                    .mode(SaveMode.Append)
                    .saveAsTable(table);
        }
    }


    /**
     * Kiểm tra dữ liệu trước khi ghi xuống Hive
     *
     * @param df DataFrame cần kiểm tra chất lượng
     * @return trả về DataFrame sau khi kiểm tra
     */
    public static Dataset<Row> qualityCheck(Dataset<Row> df) {
        // Xóa các hàng có bất kỳ giá trị null nào
        df = df.na().drop();
        return df;
    }

}
