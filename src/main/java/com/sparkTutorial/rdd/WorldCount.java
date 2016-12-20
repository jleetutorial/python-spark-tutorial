package com.sparkTutorial.rdd;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WorldCount {

    public static void main(String[] args) throws Exception {
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/word_count.text");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> counts = words.mapToPair(w -> new Tuple2<>(w, 1))
                                                   .reduceByKey((x, y) -> x + y);
        counts.saveAsTextFile("out/word_count.text");
    }
}
