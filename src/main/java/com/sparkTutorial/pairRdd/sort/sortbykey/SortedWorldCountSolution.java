package com.sparkTutorial.pairRdd.sort.sortbykey;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class SortedWorldCountSolution {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/word_count.text");
        JavaRDD<String> wordRdd = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> wordPairRdd = wordRdd.mapToPair(
                (PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));

        JavaPairRDD<String, Integer> wordToCountPairs = wordPairRdd.reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> x + y);

        JavaPairRDD<Integer, String> countToWordParis = wordToCountPairs.mapToPair(
                (PairFunction<Tuple2<String, Integer>, Integer, String>) wordToCount -> new Tuple2<>(wordToCount._2(),
                                                                                                     wordToCount._1()));

        JavaPairRDD<Integer, String> sortedCountToWordParis = countToWordParis.sortByKey(false);

        JavaPairRDD<String, Integer> sortedWordToCountPairs = sortedCountToWordParis
                .mapToPair((PairFunction<Tuple2<Integer, String>, String, Integer>) countToWord -> new Tuple2<>(countToWord._2(),
                                                                                                                countToWord._1()));

        for (Tuple2<String, Integer> wordToCount : sortedWordToCountPairs.collect()) {
            System.out.println(wordToCount._1() + " : " + wordToCount._2());

        }
    }
}
