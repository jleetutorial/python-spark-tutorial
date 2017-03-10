package com.sparkTutorial.rdd.airports;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsInUsaSolution {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("airports").setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("in/airports.text");

        JavaRDD<String> airportsInUSA = airports.filter(line -> line.split(Utils.COMMA_DELIMITER)[3].equals("\"United States\""));

        JavaRDD<String> airportsNameAndCityNames = airportsInUSA.map(line -> {
                    String[] splits = line.split(Utils.COMMA_DELIMITER);
                    return StringUtils.join(new String[]{splits[1], splits[2]}, ",");
                }
        );
        airportsNameAndCityNames.saveAsTextFile("out/airports_in_usa.text");
    }
}
