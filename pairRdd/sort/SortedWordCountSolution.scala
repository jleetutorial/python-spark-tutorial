package com.sparkTutorial.pairRdd.sort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SortedWordCountSolution {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")
    val wordRdd = lines.flatMap(line => line.split(" "))

    val wordPairRdd = wordRdd.map(word => (word, 1))
    val wordToCountPairs = wordPairRdd.reduceByKey((x, y) => x + y)

    val countToWordParis = wordToCountPairs.map(wordToCount => (wordToCount._2, wordToCount._1))

    val sortedCountToWordParis = countToWordParis.sortByKey(ascending = false)

    val sortedWordToCountPairs = sortedCountToWordParis.map(countToWord => (countToWord._2, countToWord._1))

    for ((word, count) <- sortedWordToCountPairs.collect()) println(word + " : " + count)
  }
}
