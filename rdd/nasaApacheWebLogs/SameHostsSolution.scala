package com.sparkTutorial.rdd.nasaApacheWebLogs

import org.apache.spark.{SparkConf, SparkContext}

object SameHostsSolution {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("sameHosts").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val julyFirstLogs = sc.textFile("in/nasa_19950701.tsv")
    val augustFirstLogs = sc.textFile("in/nasa_19950801.tsv")

    val julyFirstHosts = julyFirstLogs.map(line => line.split("\t")(0))
    val augustFirstHosts = augustFirstLogs.map(line => line.split("\t")(0))

    val intersection = julyFirstHosts.intersection(augustFirstHosts)

    val cleanedHostIntersection = intersection.filter(host => host != "host")
    cleanedHostIntersection.saveAsTextFile("out/nasa_logs_same_hosts.csv")
  }
}
