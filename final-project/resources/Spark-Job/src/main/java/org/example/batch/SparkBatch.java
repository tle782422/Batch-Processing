package org.example.batch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/*
FL_DATE         	        Ngày bay ở định dạng yy/mm/dd
OP_CARRIER      	        Mã định danh duy nhất của hãng hàng không
OP_CARRIER_FL_NUM   	    Số chuyến bay của hãng hàng không
ORIGIN	                    Mã duy nhất của sân bay khởi hành
DEST	                    Mã duy nhất của sân bay nơi đến
CRS_DEP_TIME	            Thời gian khởi hành dự kiến
DEP_TIME	                Giờ khởi hành thực tế
DEP_DELAY	                Tổng thời gian trễ khi khởi hành, tính bằng phút
TAXI_OUT	                Thời gian tính từ lúc khởi hành từ sân bay xuất phát đến thời điểm WHEELS_OFF
WHEELS_OFF	                Ngay khi bánh máy bay rời khỏi mặt đất
WHEELS_ON	                Khoảnh khắc bánh máy bay chạm đất
TAXI_IN             	    Thời gian từ lúc đến WHEELS_ONđến khi đến sân bay nơi đến
CRS_ARR_TIME                Thời gian đến dự kiến
ARR_TIME	                Thời gian đến thực tế
ARR_DELAY	                Tổng độ trễ khi đến nơi (âm nếu đến sớm), tính bằng phút
CANCELLED	                Chuyến bay bị hủy (1 = bị hủy)
CANCELLATION_CODE	        Lý do hủy chuyến bay: A - Hãng hàng không/Hãng vận chuyển; B – Thời tiết; C - Hệ thống hàng không quốc gia; D - Bảo mật
DIVERTED	                Máy bay hạ cánh ở sân bay khác với sân bay nơi đến
CRS_ELAPSED_TIME	        Thời gian bay dự kiến
ACTUAL_ELAPSED_TIME	        Thời gian bay thực tế ( AIR_TIME+ TAXI_IN+ TAXI_OUT)
AIR_TIME	                Khoảng thời gian máy bay đang bay. Thời gian giữa WHEELS_OFFvàWHEELS_ON
DISTANCE	                Khoảng cách giữa sân bay ORIGIN và sân bay DESTINATION
CARRIER_DELAY	            Sự chậm trễ do hãng hàng không gây ra, tính bằng phút
WEATHER_DELAY	            Sự chậm trễ do điều kiện thời tiết
NAS_DELAY	                Sự chậm trễ do Hệ thống Hàng không Quốc gia gây ra
SECURITY_DELAY	            Sự chậm trễ do kiểm tra an ninh
LATE_AIRCRAFT_DELAY	        Sự chậm trễ do máy bay đến muộn
*/


public class SparkBatch {
    public static void main(String[] args) {

        List<String> clusterSeeds = new ArrayList<>();
        clusterSeeds.add("localhost:9042");
        clusterSeeds.add("localhost:9043");

        SparkSession spark = SparkSession.builder()
                .appName("Flight Batch Analysis")
                .config("spark.cassandra.connection.host", String.join(",", clusterSeeds))
                .config("spark.cassandra.auth.username", "cassandra")
                .config("spark.cassandra.auth.password", "cassandra")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("header", true)
                .csv("hdfs://localhost:9000/output/flight_data.parquet");

        df.printSchema();

        /*
        ========================================================================================
        Delay Analysis per Carrier
        ========================================================================================
        */

        // Tính trung bình độ trễ xuất phát và đến trên toàn bộ tập dữ liệu, theo hãng hàng không.
        Dataset<Row> delayTotalDF = df.select("OP_CARRIER", "DEP_DELAY", "ARR_DELAY")
                .groupBy("OP_CARRIER")
                .agg(avg("DEP_DELAY").alias("AVG_DEP_DELAY"), avg("ARR_DELAY").alias("AVG_ARR_DELAY"))
                .select("OP_CARRIER", "AVG_DEP_DELAY", "AVG_ARR_DELAY");

        // Tính trung bình độ trễ xuất phát và đến theo từng năm, theo hãng hàng không.
        Dataset<Row> delayYearDF = df.select("OP_CARRIER", "FL_DATE", "DEP_DELAY", "ARR_DELAY")
                .withColumn("YEAR", year(col("FL_DATE_converted")))
                .groupBy("OP_CARRIER", "YEAR")
                .agg(avg("DEP_DELAY").as("AVG_DEP_DELAY"), avg("ARR_DELAY").as("AVG_ARR_DELAY"))
                .select("OP_CARRIER", "YEAR", "AVG_DEP_DELAY", "AVG_ARR_DELAY");

        // Tính trung bình độ trễ xuất phát và đến theo từng năm và tháng, theo hãng hàng không.
        Dataset<Row> delayYearMonthDF = df.select("OP_CARRIER", "FL_DATE", "DEP_DELAY", "ARR_DELAY")
                .withColumn("YEAR", year(col("FL_DATE")))
                .withColumn("MONTH", month(col("FL_DATE")))
                .groupBy("OP_CARRIER", "YEAR", "MONTH")
                .agg(avg("DEP_DELAY").as("AVG_DEP_DELAY"), avg("ARR_DELAY").as("AVG_ARR_DELAY"))
                .select("OP_CARRIER", "YEAR", "MONTH", "AVG_DEP_DELAY", "AVG_ARR_DELAY");

        // Tính trung bình độ trễ xuất phát và đến theo ngày trong tuần, theo hãng hàng không.
        Dataset<Row> delayDayOfWeekDF = df.select("OP_CARRIER", "FL_DATE", "DEP_DELAY", "ARR_DELAY")
                .withColumn("DAY_OF_WEEK", dayofweek(col("FL_DATE")))
                .groupBy("OP_CARRIER", "DAY_OF_WEEK")
                .agg(avg("DEP_DELAY").as("AVG_DEP_DELAY"), avg("ARR_DELAY").as("AVG_ARR_DELAY"))
                .select("OP_CARRIER", "DAY_OF_WEEK", "AVG_DEP_DELAY", "AVG_ARR_DELAY");

        /*
        ========================================================================================
        Delay Analysis per Source-Dest
        ========================================================================================
        */

        Dataset<Row> delayTotalSrcDestDF = df.select("ORIGIN", "DEST", "DEP_DELAY", "ARR_DELAY")
                .groupBy("ORIGIN", "DEST")  // Group by origin and destination airports
                .agg(avg("DEP_DELAY").as("AVG_DEP_DELAY"), avg("ARR_DELAY").as("AVG_ARR_DELAY")) // Calculate average delays
                .select("ORIGIN", "DEST", "AVG_DEP_DELAY", "AVG_ARR_DELAY"); // Select final columns

        Dataset<Row> delayYearSrcDestDF = df.select("ORIGIN", "DEST", "FL_DATE", "DEP_DELAY", "ARR_DELAY")
                .withColumn("YEAR", year(col("FL_DATE")))  // Extract year
                .groupBy("ORIGIN", "DEST", "YEAR")  // Group by origin, destination, and year
                .agg(avg("DEP_DELAY").as("AVG_DEP_DELAY"), avg("ARR_DELAY").as("AVG_ARR_DELAY")) // Calculate average delays
                .select("ORIGIN", "DEST", "YEAR", "AVG_DEP_DELAY", "AVG_ARR_DELAY"); // Select final columns

        Dataset<Row> delayYearMonthSrcDestDF = df.select("ORIGIN", "DEST", "FL_DATE", "DEP_DELAY", "ARR_DELAY")
                .withColumn("YEAR", year(col("FL_DATE")))  // Extract year
                .withColumn("MONTH", month(col("FL_DATE"))) // Extract month
                .groupBy("ORIGIN", "DEST", "YEAR", "MONTH")  // Group by origin, destination, year, and month
                .agg(avg("DEP_DELAY").as("AVG_DEP_DELAY"), avg("ARR_DELAY").as("AVG_ARR_DELAY")) // Calculate average delays
                .select("ORIGIN", "DEST", "YEAR", "MONTH", "AVG_DEP_DELAY", "AVG_ARR_DELAY"); // Select final columns

        Dataset<Row> delayDayOfWeekSrcDestDF = df.select("ORIGIN", "DEST", "FL_DATE", "DEP_DELAY", "ARR_DELAY")
                .withColumn("DAY_OF_WEEK", dayofweek(col("FL_DATE")))  // Extract day of week
                .groupBy("ORIGIN", "DEST", "DAY_OF_WEEK")  // Group by origin, destination, and day of week
                .agg(avg("DEP_DELAY").as("AVG_DEP_DELAY"), avg("ARR_DELAY").as("AVG_ARR_DELAY")) // Calculate average delays
                .select("ORIGIN", "DEST", "DAY_OF_WEEK", "AVG_DEP_DELAY", "AVG_ARR_DELAY"); // Select final columns

        /*
        ========================================================================================
        Cancellation & Diverted Analysis per Carrier
        ========================================================================================
        */

        // Phần trăm hủy chuyển và chậm chuyến
        Dataset<Row> cancellationDivertedTotalDF = df.select("OP_CARRIER", "CANCELLED", "DIVERTED")
                .groupBy("OP_CARRIER")
                .agg(
                        count(when(col("CANCELLED").equalTo(1), 1)).as("CANC_COUNT"), // Count cancelled flights
                        count(when(col("DIVERTED").equalTo(1), 1)).as("DIV_COUNT"), // Count diverted flights
                        count("*").as("COUNT") // Count total flights
                )
                .withColumn("DIV_PERC", col("DIV_COUNT").divide(col("COUNT")).multiply(100.0).alias("DIV_PERC")) // Calculate diversion percentage
                .withColumn("CANC_PERC", col("CANC_COUNT").divide(col("COUNT")).multiply(100.0).alias("CANC_PERC")) // Calculate cancellation percentage
                .select("OP_CARRIER", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT")
                .orderBy(col("OP_CARRIER").asc()); // Order by carrier

        Dataset<Row> cancellationDivertedYearDF = df.select("OP_CARRIER", "FL_DATE", "CANCELLED", "DIVERTED")
                .withColumn("YEAR", year(col("FL_DATE"))) // Extract year
                .groupBy("OP_CARRIER", "YEAR")
                .agg(
                        count(when(col("CANCELLED").equalTo(1), 1)).as("CANC_COUNT"),
                        count(when(col("DIVERTED").equalTo(1), 1)).as("DIV_COUNT"),
                        count("*").as("COUNT")
                )
                .withColumn("DIV_PERC", col("DIV_COUNT").divide(col("COUNT")).multiply(100.0).alias("DIV_PERC"))
                .withColumn("CANC_PERC", col("CANC_COUNT").divide(col("COUNT")).multiply(100.0).alias("CANC_PERC"))
                .select("OP_CARRIER", "YEAR", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT")
                .orderBy(col("YEAR").asc(), col("OP_CARRIER").asc()); // Order by year and carrier

        Dataset<Row> cancellationDivertedYearMonthDF = df.select("OP_CARRIER", "FL_DATE", "CANCELLED", "DIVERTED")
                .withColumn("YEAR", year(col("FL_DATE"))) // Extract year
                .withColumn("MONTH", month(col("FL_DATE"))) // Extract month
                .groupBy("OP_CARRIER", "YEAR", "MONTH")
                .agg(
                        count(when(col("CANCELLED").equalTo(1), 1)).as("CANC_COUNT"),
                        count(when(col("DIVERTED").equalTo(1), 1)).as("DIV_COUNT"),
                        count("*").as("COUNT")
                )
                .withColumn("DIV_PERC", col("DIV_COUNT").divide(col("COUNT")).multiply(100.0).alias("DIV_PERC"))
                .withColumn("CANC_PERC", col("CANC_COUNT").divide(col("COUNT")).multiply(100.0).alias("CANC_PERC"))
                .select("OP_CARRIER", "YEAR", "MONTH", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT")
                .orderBy(col("YEAR").asc(), col("MONTH").asc(), col("OP_CARRIER").asc()); // Order by year, month, and carrier

        Dataset<Row> cancellationDivertedDayOfWeekDF = df.select("OP_CARRIER", "FL_DATE", "CANCELLED", "DIVERTED")
                .withColumn("DAY_OF_WEEK", dayofweek(col("FL_DATE"))) // Extract day of week
                .groupBy("OP_CARRIER", "DAY_OF_WEEK")
                .agg(
                        count(when(col("CANCELLED").equalTo(1), 1)).as("CANC_COUNT"),
                        count(when(col("DIVERTED").equalTo(1), 1)).as("DIV_COUNT"),
                        count("*").as("COUNT")
                )
                .withColumn("DIV_PERC", col("DIV_COUNT").divide(col("COUNT")).multiply(100.0).alias("DIV_PERC"))
                .withColumn("CANC_PERC", col("CANC_COUNT").divide(col("COUNT")).multiply(100.0).alias("CANC_PERC"))
                .select("OP_CARRIER", "DAY_OF_WEEK", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT")
                .orderBy(col("DAY_OF_WEEK").asc(), col("OP_CARRIER").asc()); // Order by day of week and carrier

        /*
        ========================================================================================
        Cancellation & Diverted Analysis per Origin Destination
        ========================================================================================
        */

        Dataset<Row> cancellationDivertedTotalSrcDestDF = df.select("ORIGIN", "DEST", "CANCELLED", "DIVERTED")
                .groupBy("ORIGIN", "DEST")
                .agg(
                        count(when(col("CANCELLED").equalTo(1), 1)).as("CANC_COUNT"), // Count cancelled flights
                        count(when(col("DIVERTED").equalTo(1), 1)).as("DIV_COUNT"), // Count diverted flights
                        count("*").as("COUNT") // Count total flights
                )
                .withColumn("DIV_PERC", col("DIV_COUNT").divide(col("COUNT")).multiply(100.0).alias("DIV_PERC")) // Calculate diversion percentage
                .withColumn("CANC_PERC", col("CANC_COUNT").divide(col("COUNT")).multiply(100.0).alias("CANC_PERC")) // Calculate cancellation percentage
                .select("ORIGIN", "DEST", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT")
                .orderBy(col("ORIGIN").asc(), col("DEST").asc()); // Order by origin and destination

        Dataset<Row> cancellationDivertedYearSrcDestDF = df.select("ORIGIN", "DEST", "FL_DATE", "CANCELLED", "DIVERTED")
                .withColumn("YEAR", year(col("FL_DATE"))) // Extract year
                .groupBy("ORIGIN", "DEST", "YEAR")
                .agg(
                        count(when(col("CANCELLED").equalTo(1), 1)).as("CANC_COUNT"),
                        count(when(col("DIVERTED").equalTo(1), 1)).as("DIV_COUNT"),
                        count("*").as("COUNT")
                )
                .withColumn("DIV_PERC", col("DIV_COUNT").divide(col("COUNT")).multiply(100.0).alias("DIV_PERC"))
                .withColumn("CANC_PERC", col("CANC_COUNT").divide(col("COUNT")).multiply(100.0).alias("CANC_PERC"))
                .select("ORIGIN", "DEST", "YEAR", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT")
                .orderBy(col("YEAR").asc(), col("ORIGIN").asc(), col("DEST").asc()); // Order by year, origin, and destination

        Dataset<Row> cancellationDivertedYearMonthSrcDestDF = df.select("ORIGIN", "DEST", "FL_DATE", "CANCELLED", "DIVERTED")
                .withColumn("YEAR", year(col("FL_DATE"))) // Extract year
                .withColumn("MONTH", month(col("FL_DATE"))) // Extract month
                .groupBy("ORIGIN", "DEST", "YEAR", "MONTH")
                .agg(
                        count(when(col("CANCELLED").equalTo(1), 1)).as("CANC_COUNT"),
                        count(when(col("DIVERTED").equalTo(1), 1)).as("DIV_COUNT"),
                        count("*").as("COUNT")
                )
                .withColumn("DIV_PERC", col("DIV_COUNT").divide(col("COUNT")).multiply(100.0).alias("DIV_PERC"))
                .withColumn("CANC_PERC", col("CANC_COUNT").divide(col("COUNT")).multiply(100.0).alias("CANC_PERC"))
                .select("ORIGIN", "DEST", "YEAR", "MONTH", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT")
                .orderBy(col("YEAR").asc(), col("MONTH").asc(), col("ORIGIN").asc(), col("DEST").asc()); // Order by year, month, origin, and destination

        Dataset<Row> cancellationDivertedDayOfWeekSrcDestDF = df.select("ORIGIN", "DEST", "FL_DATE", "CANCELLED", "DIVERTED")
                .withColumn("DAY_OF_WEEK", dayofweek(col("FL_DATE"))) // Extract day of week
                .groupBy("ORIGIN", "DEST", "DAY_OF_WEEK")
                .agg(
                        count(when(col("CANCELLED").equalTo(1), 1)).as("CANC_COUNT"),
                        count(when(col("DIVERTED").equalTo(1), 1)).as("DIV_COUNT"),
                        count("*").as("COUNT")
                )
                .withColumn("DIV_PERC", col("DIV_COUNT").divide(col("COUNT")).multiply(100.0).alias("DIV_PERC"))
                .withColumn("CANC_PERC", col("CANC_COUNT").divide(col("COUNT")).multiply(100.0).alias("CANC_PERC"))
//                .withColumn("DIV_PERC", col("DIV_COUNT").cast("double").divide(col("COUNT").cast("double")).multiply(100.0).alias("DIV_PERC"))
//                .withColumn("CANC_PERC", col("CANC_COUNT").cast("double").divide(col("COUNT").cast("double")).multiply(100.0).alias("CANC_PERC"))

                .select("ORIGIN", "DEST", "DAY_OF_WEEK", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT")
                .orderBy(col("DAY_OF_WEEK").asc(), col("ORIGIN").asc(), col("DEST").asc()); // Order by day of week, origin, and destination

        /*
        ========================================================================================
        Distance Analysis per Carrier
        ========================================================================================
        */


        Dataset<Row> distTotalDF = df.select("OP_CARRIER", "DISTANCE")
                .groupBy("OP_CARRIER")
                .agg(
                        sum(col("DISTANCE")).as("TOTAL_DISTANCE") // Calculate total distance for each carrier
                )
                .select("OP_CARRIER", "TOTAL_DISTANCE")
                .orderBy(col("OP_CARRIER").asc()); // Order by carrier

        Dataset<Row> distYearDF = df.select("OP_CARRIER", "FL_DATE", "DISTANCE")
                .withColumn("YEAR", year(col("FL_DATE"))) // Extract year
                .groupBy("OP_CARRIER", "YEAR")
                .agg(
                        sum(col("DISTANCE")).as("TOTAL_DISTANCE") // Calculate total distance by carrier and year
                )
                .select("OP_CARRIER", "YEAR", "TOTAL_DISTANCE")
                .orderBy(col("YEAR").asc(), col("OP_CARRIER").asc()); // Order by year and carrier

        Dataset<Row> distYearMonthDF = df.select("OP_CARRIER", "FL_DATE", "DISTANCE")
                .withColumn("YEAR", year(col("FL_DATE"))) // Extract year
                .withColumn("MONTH", month(col("FL_DATE"))) // Extract month
                .groupBy("OP_CARRIER", "YEAR", "MONTH")
                .agg(
                        sum(col("DISTANCE")).as("TOTAL_DISTANCE") // Calculate total distance by carrier, year, and month
                )
                .select("OP_CARRIER", "YEAR", "MONTH", "TOTAL_DISTANCE")
                .orderBy(col("YEAR").asc(), col("MONTH").asc(), col("OP_CARRIER").asc()); // Order by year, month, and carrier

        Dataset<Row> distDayOfWeekDF = df.select("OP_CARRIER", "FL_DATE", "DISTANCE")
                .withColumn("DAY_OF_WEEK", dayofweek(col("FL_DATE"))) // Extract day of week
                .groupBy("OP_CARRIER", "DAY_OF_WEEK")
                .agg(
                        sum(col("DISTANCE")).as("TOTAL_DISTANCE") // Calculate total distance by carrier and day of week
                )
                .select("OP_CARRIER", "DAY_OF_WEEK", "TOTAL_DISTANCE")
                .orderBy(col("DAY_OF_WEEK").asc(), col("OP_CARRIER").asc()); // Order by day of week and carrier

        /*
        ========================================================================================
        Max consec days of Delay Analysis per Carrier
        ========================================================================================
        */


        // Truy vấn max_consec_delay_year_df
        Dataset<Row> maxConsecDelayYearDF = df.select("OP_CARRIER", "FL_DATE", "ARR_DELAY")
                .withColumn("YEAR", year(col("FL_DATE")))
                .groupBy("OP_CARRIER", "YEAR", "FL_DATE")
                .agg(avg(col("ARR_DELAY")).alias("ARR_DELAY"))
                .filter(col("ARR_DELAY").gt(0))
                .withColumn("ROW_NUMBER", row_number().over(Window.partitionBy("OP_CARRIER", "YEAR").orderBy("OP_CARRIER", "YEAR", "FL_DATE")))
                .withColumn("GRP", datediff(col("FL_DATE"), lit("1900-01-01")).minus(col("ROW_NUMBER")))
                .withColumn("GIORNI", row_number().over(Window.partitionBy("OP_CARRIER", "YEAR", "GRP").orderBy("OP_CARRIER", "YEAR", "FL_DATE")))
                .groupBy("OP_CARRIER", "YEAR")
                .agg(max(col("GIORNI")).alias("MAX_GIORNI"))
                .select("OP_CARRIER", "YEAR", "MAX_GIORNI");

        // Truy vấn max_consec_delay_year_src_dest_df
        Dataset<Row> maxConsecDelayYearSrcDestDF = df.select("ORIGIN", "DEST", "FL_DATE", "ARR_DELAY")
                .withColumn("YEAR", year(col("FL_DATE")))
                .groupBy("ORIGIN", "DEST", "YEAR", "FL_DATE")
                .agg(avg(col("ARR_DELAY")).alias("ARR_DELAY"))
                .filter(col("ARR_DELAY").gt(0))
                .withColumn("ROW_NUMBER", row_number().over(Window.partitionBy("ORIGIN", "DEST", "YEAR").orderBy("ORIGIN", "DEST", "YEAR", "FL_DATE")))
                .withColumn("GRP", datediff(col("FL_DATE"), lit("1900-01-01")).minus(col("ROW_NUMBER")))
                .withColumn("GIORNI", row_number().over(Window.partitionBy("ORIGIN", "DEST", "YEAR", "GRP").orderBy("ORIGIN", "DEST", "YEAR", "FL_DATE")))
                .groupBy("ORIGIN", "DEST", "YEAR")
                .agg(max(col("GIORNI")).alias("MAX_GIORNI"))
                .select("ORIGIN", "DEST", "YEAR", "MAX_GIORNI");



        /*
        ========================================================================================
        Group by Source-Dest and Cancellation Code
        ========================================================================================
        */

        Dataset<Row> srcDestCancCodeDF = df.select("ORIGIN", "DEST", "FL_DATE", "CANCELLED", "CANCELLATION_CODE")
                .filter(col("CANCELLED").isNotNull()) // Filter for non-null cancelled entries
                .filter(col("CANCELLED").equalTo(1)) // Filter for cancelled flights (cancelled = 1)
                .withColumn("YEAR", year(col("FL_DATE")))
                .groupBy("ORIGIN", "DEST", "YEAR", "CANCELLATION_CODE")
                .agg(count("CANCELLED").alias("NUM_CANCELLED")) // Count cancelled flights per group
                .select("ORIGIN", "DEST", "YEAR", "CANCELLATION_CODE", "NUM_CANCELLED");


        /*
        ========================================================================================
        Writing Result to Cassandra
        ========================================================================================
        */

        delayTotalDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "delay_total")
                .mode(SaveMode.Append)
                .save();

        delayYearDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "delay_year")
                .mode(SaveMode.Append)
                .save();

        delayYearMonthDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "delay_year_month")
                .mode(SaveMode.Append)
                .save();

        delayDayOfWeekDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "delay_dayofweek")
                .mode(SaveMode.Append)
                .save();



        delayTotalSrcDestDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "delay_total_src_dest")
                .mode(SaveMode.Append)
                .save();

        delayYearSrcDestDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "delay_year_src_dest")
                .mode(SaveMode.Append)
                .save();

        delayYearMonthSrcDestDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "delay_year_month_src_dest")
                .mode(SaveMode.Append)
                .save();

        delayDayOfWeekSrcDestDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "delay_dayofweek_src_dest")
                .mode(SaveMode.Append)
                .save();


        cancellationDivertedTotalDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "cancellation_diverted_total")
                .mode(SaveMode.Append)
                .save();

        cancellationDivertedYearDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "cancellation_diverted_year")
                .mode(SaveMode.Append)
                .save();

        cancellationDivertedYearMonthDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "cancellation_diverted_year_month")
                .mode(SaveMode.Append)
                .save();

        cancellationDivertedDayOfWeekDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "cancellation_diverted_dayofweek")
                .mode(SaveMode.Append)
                .save();


        cancellationDivertedTotalSrcDestDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "cancellation_diverted_total_src_dest")
                .mode(SaveMode.Append)
                .save();

        cancellationDivertedYearSrcDestDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "cancellation_diverted_year_src_dest")
                .mode(SaveMode.Append)
                .save();

        cancellationDivertedYearMonthSrcDestDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "cancellation_diverted_year_month_src_dest")
                .mode(SaveMode.Append)
                .save();

        cancellationDivertedDayOfWeekSrcDestDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "cancellation_diverted_dayofweek_src_dest")
                .mode(SaveMode.Append)
                .save();

        distTotalDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "dist_total")
                .mode(SaveMode.Append)
                .save();

        distYearDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "dist_year")
                .mode(SaveMode.Append)
                .save();

        distYearMonthDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "dist_year_month")
                .mode(SaveMode.Append)
                .save();

        distDayOfWeekDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "dist_dayofweek")
                .mode(SaveMode.Append)
                .save();


        maxConsecDelayYearDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "max_consec_delay_year")
                .mode(SaveMode.Append)
                .save();

        maxConsecDelayYearSrcDestDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "max_consec_delay_year_src_dest")
                .mode(SaveMode.Append)
                .save();

        srcDestCancCodeDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "batchkeyspace")
                .option("table", "src_dest_canc_code")
                .mode(SaveMode.Append)
                .save();

        spark.stop();

    }



}
