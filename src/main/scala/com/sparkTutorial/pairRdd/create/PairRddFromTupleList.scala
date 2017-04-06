package com.sparkTutorial.pairRdd.create

import org.apache.spark.{SparkConf, SparkContext}

object PairRddFromTupleList {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("create").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val tuple = List(("Lily", 23), ("Jack", 29), ("Mary", 29), ("James", 8))
    val pairRDD = sc.parallelize(tuple)

    pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_tuple_list")
  }
}
