package com.sparkTutorial.rdd.sumOfNumbers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SumOfNumbersSolution {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("primeNumbers").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/prime_nums.text");
        JavaRDD<String> numbers = lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

        JavaRDD<String> validNumbers = numbers.filter(number -> !number.isEmpty());

        JavaRDD<Integer> intNumbers = validNumbers.map(number -> Integer.valueOf(number));

        System.out.println("Sum is: " + intNumbers.reduce((x, y) -> x + y));
    }
}
