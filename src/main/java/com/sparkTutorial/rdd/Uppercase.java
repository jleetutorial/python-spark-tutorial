package com.sparkTutorial.rdd;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Uppercase {

    public static void main(String[] args) throws Exception {
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("uppercase").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/uppercase.text");
        JavaRDD<String> lowerCaseLines = lines.map(line -> line.toUpperCase());

        lowerCaseLines.saveAsTextFile("out/uppercase.text");
    }
}
