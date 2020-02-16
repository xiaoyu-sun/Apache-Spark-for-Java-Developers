package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf); // connection to our Spark cluster

        JavaRDD<Integer> myRdd = sc.parallelize(inputData); // load in a collection, and turn it into an RDD

        Integer result = myRdd.reduce( (value1, value2) -> value1 + value2 );

        JavaRDD<Double> sqrtRdd = myRdd.map( value -> Math.sqrt(value) );

        // the foreach method applies a void function to each element in the RDD (different from map)
//        sqrtRdd.foreach( value -> System.out.println(value) );
        sqrtRdd.collect().forEach( System.out::println );  // can also use method reference instead of Lambda expr.

        System.out.println(result);

        // how many elements in sqrtRdd
//        System.out.println(sqrtRdd.count());
        // alternatively, using just map and reduce
        JavaRDD<Long> singleIntegerRdd = sqrtRdd.map(value -> 1L);
        Long count = singleIntegerRdd.reduce( (value1, value2) -> value1 + value2 );
        System.out.println(count);

        sc.close();

    }
}
