package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        JavaRDD<String> originalLogMessages = sc.parallelize(inputData);
//
//        // building a PairRDD
//        JavaPairRDD<String, Long> pairRdd = originalLogMessages.mapToPair( rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L) );
//
//        JavaPairRDD<String, Long> sumsRdd = pairRdd.reduceByKey( (value1, value2) -> value1 + value2 );
//
//        sumsRdd.collect().forEach( tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        // a more compact version
        sc.parallelize(inputData)
                .mapToPair( rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L) )
                .reduceByKey( (value1, value2) -> value1 + value2 )
                .collect().forEach( tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        sc.close();

    }
}
