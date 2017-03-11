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
                 line -> new Tuple2<>(line.split(",")[3],
                                      Double.parseDouble(line.split(",")[2])));

        JavaPairRDD<String, AvgCount> housePriceTotal= housePricePairRdd.combineByKey(createCombiner, mergeValue, mergeCombiners);

        JavaPairRDD<String, Double> housePriceAvg = housePriceTotal.mapValues(avgCount -> avgCount.getTotal()/avgCount.getCount());

        for (Map.Entry<String, Double> housePriceAvgPair : housePriceAvg.collectAsMap().entrySet()) {
            System.out.println(housePriceAvgPair.getKey() + " : " + housePriceAvgPair.getValue());
        }
    }

    static Function<Double, AvgCount> createCombiner = x -> new AvgCount(1, x);

    static Function2<AvgCount, Double, AvgCount> mergeValue = (avgCount, x) -> new AvgCount(avgCount.getCount() + 1,
                                                                                             avgCount.getTotal() + x);
    static Function2<AvgCount, AvgCount, AvgCount> mergeCombiners =
            (avgCountA, avgCountB) -> new AvgCount(avgCountA.getCount() + avgCountB.getCount(),
                                                   avgCountA.getTotal() + avgCountB.getTotal());
}
