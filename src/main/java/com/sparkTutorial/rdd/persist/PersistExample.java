package com.sparkTutorial.rdd.persist;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;

public class PersistExample {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> inputIntegers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> integerRdd = sc.parallelize(inputIntegers);

        integerRdd.persist(StorageLevel.MEMORY_ONLY());

        integerRdd.reduce((x, y) -> x * y);

        integerRdd.count();
    }
}
