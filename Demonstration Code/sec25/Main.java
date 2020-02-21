package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Main {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

//        dataset.createOrReplaceTempView("logging_table");
//        Dataset<Row> results = spark.sql(
//                "select level, date_format(datetime, 'MMMM') as month, count(1) as total " +
//                        "from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int), level"
//        );

        dataset = dataset.select(col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));

        Object[] months = new Object[] { "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"};
        List<Object> columns = Arrays.asList(months);
        // Pivot table
        dataset = dataset.groupBy("level").pivot("month", columns).count().na().fill(0);

        dataset.show();
        spark.close();

    }
}
