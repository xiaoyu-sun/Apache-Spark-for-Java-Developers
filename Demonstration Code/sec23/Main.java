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

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

        dataset.createOrReplaceTempView("logging_table");

        Dataset<Row> results = spark.sql(
                "select level, date_format(datetime, 'MMMM') as month, count(1) as total " +
                        "from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int), level"
        );

        results.show(100);

        spark.close();

    }
}
