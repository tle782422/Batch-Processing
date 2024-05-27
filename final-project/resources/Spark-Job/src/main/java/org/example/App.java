package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.example.batch.DataFrameTransforms;
import org.example.common.util.SparkUtils;
import org.example.config.HadoopConfig;
import org.example.config.StructConfigure;

import java.util.Arrays;

public class App {
    public static void main(String[] args) {

        // Khởi tạo phiên làm việc
        SparkSession spark = SparkUtils.SparkSessionInit(
                "Test",
                "hdfs://namenode:9000/user/hive/warehouse",
                "thrift://hive-metastore:9083"
        );

        StructType scheme = StructConfigure.SparkStruct();

        // Đọc file trong HDFS
        Dataset<Row> df = spark.read()
                .option("header", true)
                .option("dateFormat", "yyyy-MM-dd")
                .option("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .schema(scheme)
                .parquet("hdfs://namenode:9000/output/flight_data.parquet");

        // Cấu hình HDFS
        Configuration hadoopConf = HadoopConfig.configureHadoop();

//        df.show(20);

        // Xử lý Biến đổi dữ liệu
        Dataset<Row> delayTotalDF = DataFrameTransforms.delayTotalDF(df);
        Dataset<Row> delayYearDF = DataFrameTransforms.delayYearDF(df);
        Dataset<Row> delayYearMonthDF = DataFrameTransforms.delayYearMonthDF(df);
        Dataset<Row> delayDayOfWeekDF = DataFrameTransforms.delayDayOfWeekDF(df);
        Dataset<Row> delayTotalSrcDestDF = DataFrameTransforms.delayTotalSrcDestDF(df);
        Dataset<Row> delayYearSrcDestDF = DataFrameTransforms.delayYearSrcDestDF(df);
        Dataset<Row> delayYearMonthSrcDestDF = DataFrameTransforms.delayYearMonthSrcDestDF(df);
        Dataset<Row> delayDayOfWeekSrcDestDF = DataFrameTransforms.delayDayOfWeekSrcDestDF(df);
        Dataset<Row> cancellationDivertedTotalDF = DataFrameTransforms.cancellationDivertedTotalDF(df);
        Dataset<Row> cancellationDivertedYearDF = DataFrameTransforms.cancellationDivertedYearDF(df);
        Dataset<Row> cancellationDivertedYearMonthDF = DataFrameTransforms.cancellationDivertedYearMonthDF(df);
        Dataset<Row> cancellationDivertedDayOfWeekDF = DataFrameTransforms.cancellationDivertedDayOfWeekDF(df);
        Dataset<Row> cancellationDivertedTotalSrcDestDF = DataFrameTransforms.cancellationDivertedTotalSrcDestDF(df);
        Dataset<Row> cancellationDivertedYearSrcDestDF = DataFrameTransforms.cancellationDivertedYearSrcDestDF(df);
        Dataset<Row> cancellationDivertedYearMonthSrcDestDF = DataFrameTransforms.cancellationDivertedYearMonthSrcDestDF(df);
        Dataset<Row> cancellationDivertedDayOfWeekSrcDestDF = DataFrameTransforms.cancellationDivertedDayOfWeekSrcDestDF(df);
        Dataset<Row> distTotalDF = DataFrameTransforms.distTotalDF(df);
        Dataset<Row> distYearDF = DataFrameTransforms.distYearDF(df);
        Dataset<Row> distYearMonthDF = DataFrameTransforms.distYearMonthDF(df);
        Dataset<Row> distDayOfWeekDF = DataFrameTransforms.distDayOfWeekDF(df);
        Dataset<Row> maxConsecDelayYearDF = DataFrameTransforms.maxConsecDelayYearDF(df);
        Dataset<Row> maxConsecDelayYearSrcDestDF = DataFrameTransforms.maxConsecDelayYearSrcDestDF(df);
        Dataset<Row> srcDestCancCodeDF = DataFrameTransforms.srcDestCancCodeDF(df);

        // Kiểm tra mẫu DataFrame về chất lượng dữ liệu trước khi ghi xuống Hive
        delayTotalDF = SparkUtils.qualityCheck(delayTotalDF);
        delayYearDF = SparkUtils.qualityCheck(delayYearDF);
        delayYearMonthDF = SparkUtils.qualityCheck(delayYearMonthDF);
        delayDayOfWeekDF = SparkUtils.qualityCheck(delayDayOfWeekDF);
        delayTotalSrcDestDF = SparkUtils.qualityCheck(delayTotalSrcDestDF);
        delayYearSrcDestDF = SparkUtils.qualityCheck(delayYearSrcDestDF);
        delayYearMonthSrcDestDF = SparkUtils.qualityCheck(delayYearMonthSrcDestDF);
        delayDayOfWeekSrcDestDF = SparkUtils.qualityCheck(delayDayOfWeekSrcDestDF);
        cancellationDivertedTotalDF = SparkUtils.qualityCheck(cancellationDivertedTotalDF);
        cancellationDivertedYearDF = SparkUtils.qualityCheck(cancellationDivertedYearDF);
        cancellationDivertedYearMonthDF = SparkUtils.qualityCheck(cancellationDivertedYearMonthDF);
        cancellationDivertedDayOfWeekDF = SparkUtils.qualityCheck(cancellationDivertedDayOfWeekDF);
        cancellationDivertedTotalSrcDestDF = SparkUtils.qualityCheck(cancellationDivertedTotalSrcDestDF);
        cancellationDivertedYearSrcDestDF = SparkUtils.qualityCheck(cancellationDivertedYearSrcDestDF);
        cancellationDivertedYearMonthSrcDestDF = SparkUtils.qualityCheck(cancellationDivertedYearMonthSrcDestDF);
        cancellationDivertedDayOfWeekSrcDestDF = SparkUtils.qualityCheck(cancellationDivertedDayOfWeekSrcDestDF);
        distTotalDF = SparkUtils.qualityCheck(distTotalDF);
        distYearDF = SparkUtils.qualityCheck(distYearDF);
        distYearMonthDF = SparkUtils.qualityCheck(distYearMonthDF);
        distDayOfWeekDF = SparkUtils.qualityCheck(distDayOfWeekDF);
        maxConsecDelayYearDF = SparkUtils.qualityCheck(maxConsecDelayYearDF);
        maxConsecDelayYearSrcDestDF = SparkUtils.qualityCheck(maxConsecDelayYearSrcDestDF);
        srcDestCancCodeDF = SparkUtils.qualityCheck(srcDestCancCodeDF);


//        delayYearDF.show();
//        delayYearMonthDF.show();
//        delayDayOfWeekDF.show();
//        delayTotalSrcDestDF.show();
//        delayYearSrcDestDF.show();
//        delayYearMonthSrcDestDF.show();
//        delayDayOfWeekSrcDestDF.show();
//        delayTotalSrcDestDF.show();
//        cancellationDivertedTotalDF.show();
//        cancellationDivertedYearDF.show();
//        cancellationDivertedYearMonthDF.show();
//        cancellationDivertedYearDF.show();
//        cancellationDivertedYearMonthSrcDestDF.show();
//        cancellationDivertedYearSrcDestDF.show();
//        cancellationDivertedDayOfWeekDF.show();
//        cancellationDivertedTotalSrcDestDF.show();
//        cancellationDivertedDayOfWeekSrcDestDF.show();
//        distTotalDF.show();
//        distYearDF.show();
//        distDayOfWeekDF.show();
//        maxConsecDelayYearDF.show();
//        maxConsecDelayYearSrcDestDF.show();
//        srcDestCancCodeDF.show();

        // Ghi vào Hive (trong Hive đã được định địa chỉ warehouse ở HDFS nên nó tự động ghi xuống HDFS)
        SparkUtils.writeToHive(delayTotalDF, "batchreports.delay_total", null);
        SparkUtils.writeToHive(delayYearDF, "batchreports.delay_year", Arrays.asList("YEAR"));
        SparkUtils.writeToHive(delayYearMonthDF, "batchreports.delay_year_month", Arrays.asList("YEAR", "MONTH"));
        SparkUtils.writeToHive(delayDayOfWeekDF, "batchreports.delay_dayofweek", Arrays.asList("DAY_OF_WEEK"));
        SparkUtils.writeToHive(delayTotalSrcDestDF, "batchreports.delay_total_src_dest", null);
        SparkUtils.writeToHive(delayYearSrcDestDF, "batchreports.delay_year_src_dest", Arrays.asList("YEAR"));
        SparkUtils.writeToHive(delayYearMonthSrcDestDF, "batchreports.delay_year_month_src_dest", Arrays.asList("YEAR", "MONTH"));
        SparkUtils.writeToHive(delayDayOfWeekSrcDestDF, "batchreports.delay_dayofweek_src_dest", Arrays.asList("DAY_OF_WEEK"));
        SparkUtils.writeToHive(cancellationDivertedTotalDF, "batchreports.cancellation_diverted_total", null);
        SparkUtils.writeToHive(cancellationDivertedYearDF, "batchreports.cancellation_diverted_year", Arrays.asList("YEAR"));
        SparkUtils.writeToHive(cancellationDivertedYearMonthDF, "batchreports.cancellation_diverted_year_month", Arrays.asList("YEAR", "MONTH"));
        SparkUtils.writeToHive(cancellationDivertedDayOfWeekDF, "batchreports.cancellation_diverted_dayofweek", Arrays.asList("DAY_OF_WEEK"));
        SparkUtils.writeToHive(cancellationDivertedTotalSrcDestDF, "batchreports.cancellation_diverted_total_src_dest", null);
        SparkUtils.writeToHive(cancellationDivertedYearSrcDestDF, "batchreports.cancellation_diverted_year_src_dest", Arrays.asList("YEAR"));
        SparkUtils.writeToHive(cancellationDivertedYearMonthSrcDestDF, "batchreports.cancellation_diverted_year_month_src_dest", Arrays.asList("YEAR", "MONTH"));
        SparkUtils.writeToHive(cancellationDivertedDayOfWeekSrcDestDF, "batchreports.cancellation_diverted_dayofweek_src_dest", Arrays.asList("DAY_OF_WEEK"));
        SparkUtils.writeToHive(distTotalDF, "batchreports.dist_total", null);
        SparkUtils.writeToHive(distYearDF, "batchreports.dist_year", Arrays.asList("YEAR"));
        SparkUtils.writeToHive(distYearMonthDF,"batchreports.dist_year_month", Arrays.asList("YEAR", "MONTH"));
        SparkUtils.writeToHive(distDayOfWeekDF, "batchreports.dist_dayofweek", Arrays.asList("DAY_OF_WEEK"));
        SparkUtils.writeToHive(maxConsecDelayYearDF, "batchreports.max_consec_delay_year", Arrays.asList("YEAR"));
        SparkUtils.writeToHive(maxConsecDelayYearSrcDestDF, "batchreports.max_consec_delay_year_src_dest", Arrays.asList("YEAR"));
        SparkUtils.writeToHive(srcDestCancCodeDF, "batchreports.src_dest_canc_code",  Arrays.asList("YEAR"));

        // Kết thúc Spark Job
        spark.stop();

    }
}
