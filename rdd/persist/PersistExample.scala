package com.sparkTutorial.rdd.persist

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object PersistExample {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("reduce").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputIntegers = List(1, 2, 3, 4, 5)
    val integerRdd = sc.parallelize(inputIntegers)

    integerRdd.persist(StorageLevel.MEMORY_ONLY)

    integerRdd.reduce((x, y) => x * y)
    integerRdd.count()
  }
}
