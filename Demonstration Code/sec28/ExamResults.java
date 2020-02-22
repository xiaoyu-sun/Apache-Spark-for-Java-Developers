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

        // Suppose we want to create a new column, indicating whether each student passes or not
        // a "pass" is defined as grades in the A/B/C range, but only A range for Biology course

        // User-defined function
        // 1st argument: function name
        // 2nd argument: lambda expression
        // 3rd argument: corresponding return type (in Spark)
        spark.udf().register("hasPassed", (String grade, String subject) -> {

            if (subject.equals("Biology")) {
                if (grade.startsWith("A")) return true;
                return false;
            }

            return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");

        }, DataTypes.BooleanType);

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

        dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade"), col("subject")));

        dataset.show();

    }

}
