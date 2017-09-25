package com.sparkTutorial.advanced.broadcast

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object UkMakerSpacesWithoutBroadcast {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("UkMakerSpaces").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)
    val postCodeMap = loadPostCodeMap()
    val makerSpaceRdd = sparkContext.textFile("in/uk-makerspaces-identifiable-data.csv")

    val regions = makerSpaceRdd
      .filter(line => line.split(Utils.COMMA_DELIMITER, -1)(0) != "Timestamp")
      .map(line => {
        getPostPrefixes(line).filter(prefix => postCodeMap.contains(prefix))
          .map(prefix => postCodeMap(prefix))
          .headOption.getOrElse("Unknow")
      })
    for ((region, count) <- regions.countByValue()) println(region + " : " + count)
  }

  def getPostPrefixes(line: String): List[String] = {
    val postcode = line.split(Utils.COMMA_DELIMITER, -1)(4)
    val cleanedPostCode = postcode.replaceAll("\\s+", "")
    (1 to cleanedPostCode.length).map(i => cleanedPostCode.substring(0, i)).toList
  }

  def loadPostCodeMap(): Map[String, String] = {
    Source.fromFile("in/uk-postcode.csv").getLines.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER, -1)
      splits(0) -> splits(7)
    }).toMap
  }
}
