package com.sparkTutorial.advanced.broadcast

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object UkMakerSpaces {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("UkMakerSpaces").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)

    val postCodeMap = sparkContext.broadcast(loadPostCodeMap())

    val makerSpaceRdd = sparkContext.textFile("in/uk-makerspaces-identifiable-data.csv")

    val regions = makerSpaceRdd
      .filter(line => line.split(Utils.COMMA_DELIMITER, -1)(0) != "Timestamp")
      .filter(line => getPostPrefix(line).isDefined)
      .map(line => postCodeMap.value.getOrElse(getPostPrefix(line).get, "Unknown"))

    for ((region, count) <- regions.countByValue()) println(region + " : " + count)
  }

  def getPostPrefix(line: String): Option[String] = {
    val splits = line.split(Utils.COMMA_DELIMITER, -1)
    val postcode = splits(4)
    if (postcode.isEmpty) None else Some(postcode.split(" ")(0))
  }

  def loadPostCodeMap(): Map[String, String] = {
    Source.fromFile("in/uk-postcode.csv").getLines.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER, -1)
      splits(0) -> splits(7)
    }).toMap
  }
}
