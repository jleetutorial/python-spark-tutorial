package com.sparkTutorial.pairRdd.join

import org.apache.spark.{SparkConf, SparkContext}

object JoinOperations {

    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("JoinOperations").setMaster("local[1]")
        val sc = new SparkContext(conf)

        val ages = sc.parallelize(List(("Tom", 29),("John", 22)))
        val addresses = sc.parallelize(List(("James", "USA"), ("John", "UK")))

        val join = ages.join(addresses)
        join.saveAsTextFile("out/age_address_join.text")

        val leftOuterJoin = ages.leftOuterJoin(addresses)
        leftOuterJoin.saveAsTextFile("out/age_address_left_out_join.text")

        val rightOuterJoin = ages.rightOuterJoin(addresses)
        rightOuterJoin.saveAsTextFile("out/age_address_right_out_join.text")

        val fullOuterJoin = ages.fullOuterJoin(addresses)
        fullOuterJoin.saveAsTextFile("out/age_address_full_out_join.text")
    }
}
