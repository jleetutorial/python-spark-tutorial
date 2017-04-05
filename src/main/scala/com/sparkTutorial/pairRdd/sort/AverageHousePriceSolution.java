package com.sparkTutorial.pairRdd.sort;


import com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice.AvgCount;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AverageHousePriceSolution {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/RealEstate.csv");
        JavaRDD<String> cleanedLines = lines.filter(line -> !line.contains("Bedrooms"));

        JavaPairRDD<Integer, AvgCount> housePricePairRdd = cleanedLines.mapToPair(
                line -> new Tuple2<>(Integer.valueOf(line.split(",")[3]),
                        new AvgCount(1, Double.parseDouble(line.split(",")[2]))));

        JavaPairRDD<Integer, AvgCount> housePriceTotal = housePricePairRdd.reduceByKey(
                (x, y) -> new AvgCount(x.getCount() + y.getCount(), x.getTotal() + y.getTotal()));

        JavaPairRDD<Integer, Double> housePriceAvg = housePriceTotal.mapValues(avgCount -> avgCount.getTotal()/avgCount.getCount());

        JavaPairRDD<Integer, Double> sortedHousePriceAvg = housePriceAvg.sortByKey();

        for (Tuple2<Integer, Double> price : sortedHousePriceAvg.collect()) {
            System.out.println(price._1() + " : " + price._2());
        }
    }

}
