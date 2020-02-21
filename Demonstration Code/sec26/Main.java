package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class ExamResults {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

        dataset = dataset.groupBy("subject").agg(max(col("score").cast(DataTypes.IntegerType)).alias("max score"),
                                                      min(col("score").cast(DataTypes.IntegerType)).alias("min score"));

        // In fact, we do not need to cast the data types for the agg method:
//        dataset = dataset.groupBy("subject").agg(max(col("score")).alias("max score"),
//                                                      min(col("score")).alias("min score"));
        dataset.show();

    }

}
