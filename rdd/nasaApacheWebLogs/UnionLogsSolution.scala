package com.sparkTutorial.rdd.nasaApacheWebLogs

import org.apache.spark.{SparkConf, SparkContext}

object UnionLogsSolution {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("unionLogs").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val julyFirstLogs = sc.textFile("in/nasa_19950701.tsv")
    val augustFirstLogs = sc.textFile("in/nasa_19950801.tsv")

    val aggregatedLogLines = julyFirstLogs.union(augustFirstLogs)

    val cleanLogLines = aggregatedLogLines.filter(line => isNotHeader(line))

    val sample = cleanLogLines.sample(withReplacement = true, fraction = 0.1)

    sample.saveAsTextFile("out/sample_nasa_logs.csv")
  }

  def isNotHeader(line: String): Boolean = !(line.startsWith("host") && line.contains("bytes"))
}
