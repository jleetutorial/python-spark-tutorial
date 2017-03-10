package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UnionLogsSolution {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("unionLogs").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> julyFirstLogs = sc.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> augustFirstLogs = sc.textFile("in/nasa_19950801.tsv");

        JavaRDD<String> aggregatedLogLines = julyFirstLogs.union(augustFirstLogs);

        JavaRDD<String> cleanLogLines = aggregatedLogLines.filter(line -> isNotHeader(line));

        JavaRDD<String> sample = cleanLogLines.sample(true, 0.1);

        sample.saveAsTextFile("out/sample_nasa_logs.csv");
    }

    private static boolean isNotHeader(String line) {
        return !(line.startsWith("host") && line.contains("bytes"));
    }
}
