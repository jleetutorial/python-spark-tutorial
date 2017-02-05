package com.sparkTutorial.pairRdd.groupbykey;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class AirportsSolution {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("airports").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/airports.text");

        JavaPairRDD<String, String> CountryAndAirportNameAndPair = lines.mapToPair((PairFunction<String, String, String>) airport -> new Tuple2<>(airport.split(",")[3], airport.split(",")[1]));

        JavaPairRDD<String, Iterable<String>> AirportsByCountry = CountryAndAirportNameAndPair.groupByKey();

        for (Tuple2<String, Iterable<String>> airports : AirportsByCountry.collect()) {
            System.out.print(airports._1() + " : [");
            for (String s : airports._2()) {
                System.out.print(s + ", ");
            }
            System.out.println("]");
        }
    }
}
