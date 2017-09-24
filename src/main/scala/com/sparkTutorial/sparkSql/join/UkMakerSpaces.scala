package com.sparkTutorial.sparkSql.join

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}

object UkMakerSpaces {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val session = SparkSession.builder().appName("UkMakerSpaces").master("local[*]").getOrCreate()

    val makerSpace = session.read.option("header", "true").csv("in/uk-makerspaces-identifiable-data.csv")

    val postCode = session.read.option("header", "true").csv("in/uk-postcode.csv")
       .withColumn("PostCode", functions.concat_ws("", functions.col("PostCode"), functions.lit(" ")))

    System.out.println("=== Print 20 records of makerspace table ===")
    makerSpace.select("Name of makerspace", "Postcode").show()

    System.out.println("=== Print 20 records of postcode table ===")
    postCode.show()

    val joined = makerSpace.join(postCode, makerSpace.col("Postcode").startsWith(postCode.col("Postcode")), "left_outer")

    System.out.println("=== Group by Region ===")
    joined.groupBy("Region").count().show(200)
  }
}
