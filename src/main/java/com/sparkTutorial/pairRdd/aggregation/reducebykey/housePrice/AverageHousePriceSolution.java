package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Map;

public class AverageHousePriceSolution {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/RealEstate.csv");

        JavaRDD<String> cleanedLines = lines.filter(line -> !line.contains("Bedrooms"));

        JavaPairRDD<String, AvgCount> housePricePairRdd = cleanedLines.mapToPair(
                (PairFunction<String, String, AvgCount>) line ->
                new Tuple2<>(line.split(",")[3],
                             new AvgCount(1, Double.parseDouble(line.split(",")[2]))));

        JavaPairRDD<String, AvgCount> housePriceTotal = housePricePairRdd.reduceByKey(
                (Function2<AvgCount, AvgCount, AvgCount>) (x, y) ->
                new AvgCount(x.getCount() + y.getCount(), x.getTotal() + y.getTotal()));

        System.out.println("housePriceTotal: ");
        for (Map.Entry<String, AvgCount> housePriceTotalPair : housePriceTotal.collectAsMap().entrySet()) {
            System.out.println(housePriceTotalPair.getKey() + " : " + housePriceTotalPair.getValue());

        }

        JavaPairRDD<String, Double> housePriceAvg = housePriceTotal.mapToPair(
                (PairFunction<Tuple2<String, AvgCount>, String, Double>) total ->
                 new Tuple2<>(total._1(), total._2().getTotal()/total._2().getCount()));

        System.out.println("housePriceAvg: ");
        for (Map.Entry<String, Double> housePriceAvgPair : housePriceAvg.collectAsMap().entrySet()) {
            System.out.println(housePriceAvgPair.getKey() + " : " + housePriceAvgPair.getValue());

        }
    }

}
