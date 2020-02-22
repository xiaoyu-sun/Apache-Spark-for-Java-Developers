package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;

public class Main {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

        // Using a UDF in Spark SQL
        SimpleDateFormat input = new SimpleDateFormat("MMMM");
        SimpleDateFormat output = new SimpleDateFormat("M");

//        spark.udf().register("monthNum", (String month) -> {
//            java.util.Date inputDate = input.parse(month);
//            return Integer.parseInt(output.format(inputDate));
//        }, DataTypes.IntegerType);

        spark.udf().register("monthNum", (String month) -> {
            try {
                if (month.equals("January")) return 1;
                if (month.equals("February")) return 2;
                if (month.equals("March")) return 3;
                if (month.equals("April")) return 4;
                if (month.equals("May")) return 5;
                if (month.equals("June")) return 6;
                if (month.equals("July")) return 7;
                if (month.equals("August")) return 8;
                if (month.equals("September")) return 9;
                if (month.equals("October")) return 10;
                if (month.equals("November")) return 11;
                if (month.equals("December")) return 12;
                return output.format(input.parse(month));
            } catch (Exception e) {
                return 0;
            }
        }, DataTypes.IntegerType);


        dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> results = spark.sql(
                "select level, date_format(datetime, 'MMMM') as month, count(1) as total " +
                        "from logging_table group by level, month order by monthNum(month), level"
        );

        results.show(100);
        spark.close();

    }
}
