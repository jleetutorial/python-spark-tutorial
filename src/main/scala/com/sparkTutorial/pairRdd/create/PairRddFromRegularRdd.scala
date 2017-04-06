package com.sparkTutorial.pairRdd.create

import org.apache.spark.{SparkConf, SparkContext}

object PairRddFromRegularRdd {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("create").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val inputStrings = List("Lily 23", "Jack 29", "Mary 29", "James 8")
    val regularRDDs = sc.parallelize(inputStrings)

    val pairRDD = regularRDDs.map(s => (s.split(" ")(0), s.split(" ")(1)))
    pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_regular_rdd")
  }
}
