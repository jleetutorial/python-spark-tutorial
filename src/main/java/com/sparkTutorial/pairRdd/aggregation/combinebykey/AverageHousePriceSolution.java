package com.sparkTutorial.pairRdd.aggregation.combinebykey;

import com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice.AvgCount;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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

        JavaPairRDD<String, Double> housePricePairRdd = cleanedLines.mapToPair(
                (PairFunction<String, String, Double>) line ->
                new Tuple2<>(line.split(",")[3],
                             Double.parseDouble(line.split(",")[2])));

        JavaPairRDD<String, AvgCount> housePriceTotal= housePricePairRdd.combineByKey(createCombiner, mergeValue, mergeCombiners);

        JavaPairRDD<String, Double> housePriceAvg = housePriceTotal.mapToPair(
                (PairFunction<Tuple2<String, AvgCount>, String, Double>) total ->
                 new Tuple2<>(total._1(), total._2().getTotal()/total._2().getCount()));

        for (Map.Entry<String, Double> housePriceAvgPair : housePriceAvg.collectAsMap().entrySet()) {
            System.out.println(housePriceAvgPair.getKey() + " : " + housePriceAvgPair.getValue());

        }
    }

    static Function<Double, AvgCount> createCombiner = (Function<Double, AvgCount>) x -> new AvgCount(1, x);

    static Function2<AvgCount, Double, AvgCount> mergeValue
            = (Function2<AvgCount, Double, AvgCount>) (avgCount, x) -> new AvgCount(avgCount.getCount() + 1,
                                                                                    avgCount.getTotal() + x);
    static Function2<AvgCount, AvgCount, AvgCount> mergeCombiners
            = (Function2<AvgCount, AvgCount, AvgCount>) (avgCountA, avgCountB) -> new AvgCount(avgCountA.getCount() + avgCountB.getCount(),
                                                                                               avgCountA.getTotal() + avgCountB.getTotal());

}
