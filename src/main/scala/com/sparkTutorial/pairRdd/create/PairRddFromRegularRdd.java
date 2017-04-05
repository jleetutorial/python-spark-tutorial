package com.sparkTutorial.pairRdd.create;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class PairRddFromRegularRdd {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("create").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> inputStrings = Arrays.asList("Lily 23", "Jack 29", "Mary 29", "James 8");

        JavaRDD<String> regularRDDs = sc.parallelize(inputStrings);

        JavaPairRDD<String, Integer> pairRDD = regularRDDs.mapToPair(getNameAndAgePair());

        pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_regular_rdd");
    }

    private static PairFunction<String, String, Integer> getNameAndAgePair() {
        return (PairFunction<String, String, Integer>) s -> new Tuple2<>(s.split(" ")[0],
                                                                         Integer.valueOf(s.split(" ")[1]));
    }
}
