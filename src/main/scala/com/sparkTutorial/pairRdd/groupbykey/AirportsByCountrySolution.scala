package com.sparkTutorial.pairRdd.groupbykey

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByCountrySolution {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("airports").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/airports.text")

    val countryAndAirportNameAndPair = lines.map(airport => (airport.split(Utils.COMMA_DELIMITER)(3),
                                                             airport.split(Utils.COMMA_DELIMITER)(1)))

    val airportsByCountry = countryAndAirportNameAndPair.groupByKey()

    for ((country, airportName) <- airportsByCountry.collectAsMap()) println(country + ": " + airportName.toList)
  }
}
