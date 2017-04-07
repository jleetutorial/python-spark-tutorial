//package com.sparkTutorial.advanced.broadcast
//
//import com.sparkTutorial.rdd.commons.Utils
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.io.Source
//
//object UkMakerSpaces {
//
//  def main(args: Array[String]) {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val conf = new SparkConf().setAppName("UkMakerSpaces").setMaster("local[1]")
//    val sparkContext = new SparkContext(conf)
//
//    val postCodeMap = sparkContext.broadcast(loadPostCodeMap())
//
//    val makerSpaceRdd = sparkContext.textFile("in/uk-makerspaces-identifiable-data.csv")
//
//    val regions = makerSpaceRdd.filter(line => line.split(Utils.COMMA_DELIMITER, -1)(0) != "Timestamp")
//      .map(line => {
//        val postPrefix = getPostPrefix(line)
//        if (postPrefix.isDefined && (postCodeMap.value contains postPrefix.get)) {
//          postCodeMap.value(postPrefix.get)
//        }
//        "Unknown"
//      })
//    for ((region, count) <- regions.countByValue()) println(region + " : " + count)
//  }
//
//
//  def getPostPrefix(line: String): Option[String] = {
//    val splits = line.split(Utils.COMMA_DELIMITER, -1)
//    val postcode = splits(4)
//    if (postcode.isEmpty) None else Some(postcode.split(" ")(0))
//  }
//
//  def loadPostCodeMap(): Map[String, String] = {
//    var postCodeMap = Map[String, String]()
//    for (line <- Source.fromFile("in/uk-postcode.csv").getLines) {
//      val splits = line.split(Utils.COMMA_DELIMITER, -1)
//      postCodeMap += (splits(0) -> splits(7))
//    }
//    postCodeMap
//  }
//}
