package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

//        Dataset<Row> modernArtResults = dataset.filter(" subject = 'Modern Art' AND year >= 2007 ");


        // Now we write full SQL-style expressions:
        dataset.createOrReplaceTempView("my_students_table");

        Dataset<Row> results = spark.sql("select distinct year from my_students_table order by year desc");

        results.show();

        spark.close();

    }
}
