package com.sparkTutorial.pairRdd.aggregation.reducebykey

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")
    val wordRdd = lines.flatMap(line => line.split(" "))
    val wordPairRdd = wordRdd.map(word => (word, 1))

    val wordCounts = wordPairRdd.reduceByKey((x, y) => x + y)
    for ((word, count) <- wordCounts.collect()) println(word + " : " + count)
  }
}
