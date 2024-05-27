package org.example.batch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class DataFrameTransforms {


    public static Dataset<Row> delayTotalDF(Dataset<Row> df) {
        Dataset<Row> delayTotalDF = df.select("OP_CARRIER", "DEP_DELAY", "ARR_DELAY")
                .groupBy("OP_CARRIER")
                .agg(avg("DEP_DELAY").alias("AVG_DEP_DELAY"), avg("ARR_DELAY").alias("AVG_ARR_DELAY"))
                .select("OP_CARRIER", "AVG_DEP_DELAY", "AVG_ARR_DELAY");
        return delayTotalDF;
    }

    public static Dataset<Row> delayYearDF(Dataset<Row> df) {
        Dataset<Row> delayYearDF = df.select("OP_CARRIER", "FL_DATE", "DEP_DELAY", "ARR_DELAY")
                .withColumn("YEAR", year(col("FL_DATE")))
                .groupBy("OP_CARRIER", "YEAR")
                .agg(avg("DEP_DELAY").as("AVG_DEP_DELAY"), avg("ARR_DELAY").as("AVG_ARR_DELAY"))
                .select("OP_CARRIER", "YEAR", "AVG_DEP_DELAY", "AVG_ARR_DELAY");
        return delayYearDF;
    }

    public static Dataset<Row> delayYearMonthDF(Dataset<Row> df) {
        Dataset<Row> delayYearMonthDF = df.select("OP_CARRIER", "FL_DATE", "DEP_DELAY", "ARR_DELAY")
                .withColumn("YEAR", year(col("FL_DATE")))
                .withColumn("MONTH", month(col("FL_DATE")))
                .groupBy("OP_CARRIER", "YEAR", "MONTH")
                .agg(avg("DEP_DELAY").as("AVG_DEP_DELAY"), avg("ARR_DELAY").as("AVG_ARR_DELAY"))
                .select("OP_CARRIER", "YEAR", "MONTH", "AVG_DEP_DELAY", "AVG_ARR_DELAY");
        return delayYearMonthDF;
    }

    public static Dataset<Row> delayDayOfWeekDF(Dataset<Row> df) {
        Dataset<Row> delayDayOfWeekDF = df.select("OP_CARRIER", "FL_DATE", "DEP_DELAY", "ARR_DELAY")
                .withColumn("DAY_OF_WEEK", dayofweek(col("FL_DATE")))
                .groupBy("OP_CARRIER", "DAY_OF_WEEK")
                .agg(avg("DEP_DELAY").as("AVG_DEP_DELAY"), avg("ARR_DELAY").as("AVG_ARR_DELAY"))
                .select("OP_CARRIER", "DAY_OF_WEEK", "AVG_DEP_DELAY", "AVG_ARR_DELAY");
        return delayDayOfWeekDF;
    }

    public static Dataset<Row> delayTotalSrcDestDF(Dataset<Row> df) {
        Dataset<Row> delayTotalSrcDestDF = df.select("ORIGIN", "DEST", "DEP_DELAY", "ARR_DELAY")
                .groupBy("ORIGIN", "DEST")  // Group by origin and destination airports
                .agg(avg("DEP_DELAY").as("AVG_DEP_DELAY"), avg("ARR_DELAY").as("AVG_ARR_DELAY")) // Calculate average delays
                .select("ORIGIN", "DEST", "AVG_DEP_DELAY", "AVG_ARR_DELAY"); // Select final columns
        return delayTotalSrcDestDF;
    }

    public static Dataset<Row> delayYearSrcDestDF(Dataset<Row> df) {
        Dataset<Row> delayYearSrcDestDF = df.select("ORIGIN", "DEST", "FL_DATE", "DEP_DELAY", "ARR_DELAY")
                .withColumn("YEAR", year(col("FL_DATE")))  // Extract year
                .groupBy("ORIGIN", "DEST", "YEAR")  // Group by origin, destination, and year
                .agg(avg("DEP_DELAY").as("AVG_DEP_DELAY"), avg("ARR_DELAY").as("AVG_ARR_DELAY")) // Calculate average delays
                .select("ORIGIN", "DEST", "YEAR", "AVG_DEP_DELAY", "AVG_ARR_DELAY"); // Select final columns
        return delayYearSrcDestDF;
    }

    public static Dataset<Row> delayYearMonthSrcDestDF(Dataset<Row> df) {
        Dataset<Row> delayYearMonthSrcDestDF = df.select("ORIGIN", "DEST", "FL_DATE", "DEP_DELAY", "ARR_DELAY")
                .withColumn("YEAR", year(col("FL_DATE")))  // Extract year
                .withColumn("MONTH", month(col("FL_DATE"))) // Extract month
                .groupBy("ORIGIN", "DEST", "YEAR", "MONTH")  // Group by origin, destination, year, and month
                .agg(avg("DEP_DELAY").as("AVG_DEP_DELAY"), avg("ARR_DELAY").as("AVG_ARR_DELAY")) // Calculate average delays
                .select("ORIGIN", "DEST", "YEAR", "MONTH", "AVG_DEP_DELAY", "AVG_ARR_DELAY"); // Select final columns
        return delayYearMonthSrcDestDF;
    }

    public static Dataset<Row> delayDayOfWeekSrcDestDF(Dataset<Row> df) {
        Dataset<Row> delayDayOfWeekSrcDestDF = df.select("ORIGIN", "DEST", "FL_DATE", "DEP_DELAY", "ARR_DELAY")
                .withColumn("DAY_OF_WEEK", dayofweek(col("FL_DATE")))  // Extract day of week
                .groupBy("ORIGIN", "DEST", "DAY_OF_WEEK")  // Group by origin, destination, and day of week
                .agg(avg("DEP_DELAY").as("AVG_DEP_DELAY"), avg("ARR_DELAY").as("AVG_ARR_DELAY")) // Calculate average delays
                .select("ORIGIN", "DEST", "DAY_OF_WEEK", "AVG_DEP_DELAY", "AVG_ARR_DELAY"); // Select final columns
        return delayDayOfWeekSrcDestDF;
    }

    public static Dataset<Row> cancellationDivertedTotalDF(Dataset<Row> df) {
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
        return cancellationDivertedTotalDF;
    }

    public static Dataset<Row> cancellationDivertedYearDF(Dataset<Row> df) {
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
        return cancellationDivertedYearDF;
    }

    public static Dataset<Row> cancellationDivertedYearMonthDF(Dataset<Row> df) {
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
        return cancellationDivertedYearMonthDF;
    }

    public static Dataset<Row> cancellationDivertedDayOfWeekDF(Dataset<Row> df) {
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
        return cancellationDivertedDayOfWeekDF;
    }

    public static Dataset<Row> cancellationDivertedTotalSrcDestDF(Dataset<Row> df) {

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
        return cancellationDivertedTotalSrcDestDF;
    }

    public static Dataset<Row> cancellationDivertedYearSrcDestDF(Dataset<Row> df) {
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
        return  cancellationDivertedYearSrcDestDF;
    }

    public static Dataset<Row> cancellationDivertedYearMonthSrcDestDF(Dataset<Row> df) {
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
        return cancellationDivertedYearMonthSrcDestDF;
    }

    public static Dataset<Row> cancellationDivertedDayOfWeekSrcDestDF(Dataset<Row> df) {
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
        return cancellationDivertedDayOfWeekSrcDestDF;
    }

    public static Dataset<Row> distTotalDF(Dataset<Row> df) {
        Dataset<Row> distTotalDF = df.select("OP_CARRIER", "DISTANCE")
                .groupBy("OP_CARRIER")
                .agg(
                        sum(col("DISTANCE")).as("TOTAL_DISTANCE") // Calculate total distance for each carrier
                )
                .select("OP_CARRIER", "TOTAL_DISTANCE")
                .orderBy(col("OP_CARRIER").asc()); // Order by carrier
        return distTotalDF;
    }

    public static Dataset<Row> distYearDF(Dataset<Row> df) {
        Dataset<Row> distYearDF = df.select("OP_CARRIER", "FL_DATE", "DISTANCE")
                .withColumn("YEAR", year(col("FL_DATE"))) // Extract year
                .groupBy("OP_CARRIER", "YEAR")
                .agg(
                        sum(col("DISTANCE")).as("TOTAL_DISTANCE") // Calculate total distance by carrier and year
                )
                .select("OP_CARRIER", "YEAR", "TOTAL_DISTANCE")
                .orderBy(col("YEAR").asc(), col("OP_CARRIER").asc()); // Order by year and carrier
        return distYearDF;
    }

    public static Dataset<Row> distYearMonthDF(Dataset<Row> df) {
        Dataset<Row> distYearMonthDF = df.select("OP_CARRIER", "FL_DATE", "DISTANCE")
                .withColumn("YEAR", year(col("FL_DATE")))
                .withColumn("MONTH", month(col("FL_DATE")))
                .groupBy("OP_CARRIER", "YEAR", "MONTH")
                .agg(sum(col("DISTANCE")).alias("TOTAL_DISTANCE"))
                .select("OP_CARRIER", "YEAR", "MONTH", "TOTAL_DISTANCE")
                .orderBy("YEAR", "MONTH", "OP_CARRIER");
        return distYearMonthDF;
    }

    public static Dataset<Row> distDayOfWeekDF(Dataset<Row> df) {
        Dataset<Row> distDayOfWeekDF = df.select("OP_CARRIER", "FL_DATE", "DISTANCE")
                .withColumn("DAY_OF_WEEK", dayofweek(col("FL_DATE"))) // Extract day of week
                .groupBy("OP_CARRIER", "DAY_OF_WEEK")
                .agg(
                        sum(col("DISTANCE")).as("TOTAL_DISTANCE") // Calculate total distance by carrier and day of week
                )
                .select("OP_CARRIER", "DAY_OF_WEEK", "TOTAL_DISTANCE")
                .orderBy(col("DAY_OF_WEEK").asc(), col("OP_CARRIER").asc()); // Order by day of week and carrier
        return distDayOfWeekDF;
    }

    public static Dataset<Row> maxConsecDelayYearDF(Dataset<Row> df) {
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
        return  maxConsecDelayYearDF;
    }

    public static Dataset<Row> maxConsecDelayYearSrcDestDF(Dataset<Row> df) {
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
        return maxConsecDelayYearSrcDestDF;
    }


    public static Dataset<Row> srcDestCancCodeDF(Dataset<Row> df) {
        Dataset<Row> srcDestCancCodeDF = df.select("ORIGIN", "DEST", "FL_DATE", "CANCELLED", "CANCELLATION_CODE")
                .filter(col("CANCELLED").isNotNull()) // Filter for non-null cancelled entries
                .filter(col("CANCELLED").equalTo(1)) // Filter for cancelled flights (cancelled = 1)
                .withColumn("YEAR", year(col("FL_DATE")))
                .groupBy("ORIGIN", "DEST", "YEAR", "CANCELLATION_CODE")
                .agg(count("CANCELLED").alias("NUM_CANCELLED")) // Count cancelled flights per group
                .select("ORIGIN", "DEST", "YEAR", "CANCELLATION_CODE", "NUM_CANCELLED");
        return srcDestCancCodeDF;
    }

}


