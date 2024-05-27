package org.example.config;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class StructConfigure {

    public static StructType SparkStruct(){
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("FL_DATE", DataTypes.DateType, true),
                DataTypes.createStructField("OP_CARRIER", DataTypes.StringType, true),
                DataTypes.createStructField("OP_CARRIER_FL_NUM", DataTypes.IntegerType, true),
                DataTypes.createStructField("ORIGIN", DataTypes.StringType, true),
                DataTypes.createStructField("DEST", DataTypes.StringType, true),
                DataTypes.createStructField("CRS_DEP_TIME", DataTypes.IntegerType, true),
                DataTypes.createStructField("DEP_TIME", DataTypes.DoubleType, true),
                DataTypes.createStructField("DEP_DELAY", DataTypes.DoubleType, true),
                DataTypes.createStructField("TAXI_OUT", DataTypes.DoubleType, true),
                DataTypes.createStructField("WHEELS_OFF", DataTypes.DoubleType, true),
                DataTypes.createStructField("WHEELS_ON", DataTypes.DoubleType, true),
                DataTypes.createStructField("TAXI_IN", DataTypes.DoubleType, true),
                DataTypes.createStructField("CRS_ARR_TIME", DataTypes.IntegerType, true),
                DataTypes.createStructField("ARR_TIME", DataTypes.DoubleType, true),
                DataTypes.createStructField("ARR_DELAY", DataTypes.DoubleType, true),
                DataTypes.createStructField("CANCELLED", DataTypes.DoubleType, true),
                DataTypes.createStructField("CANCELLATION_CODE", DataTypes.StringType, true),
                DataTypes.createStructField("DIVERTED", DataTypes.DoubleType, true),
                DataTypes.createStructField("CRS_ELAPSED_TIME", DataTypes.DoubleType, true),
                DataTypes.createStructField("ACTUAL_ELAPSED_TIME", DataTypes.DoubleType, true),
                DataTypes.createStructField("AIR_TIME", DataTypes.DoubleType, true),
                DataTypes.createStructField("DISTANCE", DataTypes.DoubleType, true),
                DataTypes.createStructField("CARRIER_DELAY", DataTypes.DoubleType, true),
                DataTypes.createStructField("WEATHER_DELAY", DataTypes.DoubleType, true),
                DataTypes.createStructField("NAS_DELAY", DataTypes.DoubleType, true),
                DataTypes.createStructField("SECURITY_DELAY", DataTypes.DoubleType, true),
                DataTypes.createStructField("LATE_AIRCRAFT_DELAY", DataTypes.DoubleType, true)
        });
        return schema;
    }

}
