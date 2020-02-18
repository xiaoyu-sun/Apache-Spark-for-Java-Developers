package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Main {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

//        dataset.show();
//
//        long numberOfRows = dataset.count();
//        System.out.println("There are " + numberOfRows + " records");
//
//        Row firstRow = dataset.first();
//        String subject = firstRow.getAs("subject").toString();
//        System.out.println(subject);
//
//        int year = Integer.parseInt(firstRow.getAs("year"));
//        System.out.println("The year was " + year);



        // 1. filters using expressions (SQL-style)
//        Dataset<Row> modernArtResults = dataset.filter(" subject = 'Modern Art' AND year >= 2007 ");

        // 2. filters using lambdas
//        Dataset<Row> modernArtResults = dataset.filter((FilterFunction<Row>) row -> row.getAs("subject").equals("Modern Art")
//                && Integer.parseInt(row.getAs("year")) >= 2007);

        // 3. filters using columns
        Dataset<Row> modernArtResults = dataset.filter(col("subject").equalTo("Modern Art")
                                                                    .and(col("year").geq(2007)));
        modernArtResults.show();

        spark.close();

    }
}
