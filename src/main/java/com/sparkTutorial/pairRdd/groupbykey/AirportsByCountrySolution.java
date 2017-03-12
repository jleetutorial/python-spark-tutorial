package com.sparkTutorial.pairRdd.groupbykey;

import com.google.common.collect.Iterables;
import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class AirportsByCountrySolution {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("airports").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/airports.text");

        JavaPairRDD<String, String> CountryAndAirportNameAndPair =
                lines.mapToPair((PairFunction<String, String, String>) airport ->
                        new Tuple2<>(airport.split(Utils.COMMA_DELIMITER)[3],
                                     airport.split(Utils.COMMA_DELIMITER)[1]));

        JavaPairRDD<String, Iterable<String>> AirportsByCountry = CountryAndAirportNameAndPair.groupByKey();

        for (Tuple2<String, Iterable<String>> airports : AirportsByCountry.collect()) {
            System.out.println(airports._1() + " : " + iterableToString(airports._2()));
        }
    }

    private static String iterableToString(Iterable<String> iterable) {
        return Arrays.toString(Iterables.toArray(iterable, String.class));
    }
}
