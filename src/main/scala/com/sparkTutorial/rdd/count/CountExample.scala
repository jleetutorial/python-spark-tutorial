package com.sparkTutorial.rdd.count

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object CountExample {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("count").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
    val wordRdd = sc.parallelize(inputWords)
    println("Count: " + wordRdd.count())

    val wordCountByValue = wordRdd.countByValue()
    println("CountByValue:")

    for ((word, count) <- wordCountByValue) println(word + " : " + count)
  }
}
