package com.sparkTutorial.pairRdd.groupbykey;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class GroupByKeyVsReduceByKey {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("GroupByKeyVsReduceByKey").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> words = Arrays.asList("one", "two", "two", "three", "three", "three");

        JavaPairRDD<String, Integer> wordsPairRdd = sc.parallelize(words).mapToPair(word -> new Tuple2<>(word, 1));

        List<Tuple2<String, Integer>> wordCountsWithReduce = wordsPairRdd.reduceByKey((x, y) -> x + y).collect();

        List<Tuple2<String, Integer>> wordCountsWithGroup = wordsPairRdd.groupByKey()
                .mapToPair(word -> new Tuple2<>(word._1(), getSum(word._2()))).collect();
    }

    private static int getSum(Iterable<Integer> integers) {
        int sum = 0;
        for (Integer integer : integers) {
            sum = + integer;
        }
        return sum;
    }
}

