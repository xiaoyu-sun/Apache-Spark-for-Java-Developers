package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

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

        // if we use the following groupBy, the "monthnum" will be discarded because no aggregation is performed on it
//        dataset = dataset.groupBy(col("level"), col("month")).count();
        // The following would work. Note that "monthnum" always matches "month".
        dataset = dataset.groupBy(col("level"), col("month"), col("monthnum")).count();
        dataset = dataset.orderBy("monthnum", "level");
        dataset = dataset.drop("monthnum");

        dataset.show(100);
        spark.close();

    }
}
