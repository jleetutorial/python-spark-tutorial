package com.sparkTutorial.rdd.reduce
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object ReduceExample {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("reduce").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputIntegers = List(1, 2, 3, 4, 5)
    val integerRdd = sc.parallelize(inputIntegers)

    val product = integerRdd.reduce((x, y) => x * y)
    println("product is :" + product)
  }
}
