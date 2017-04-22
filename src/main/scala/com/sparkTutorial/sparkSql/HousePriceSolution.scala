package com.sparkTutorial.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object HousePriceSolution {

  val PRICE = "Price"
  val PRICE_SQ_FT = "Price SQ Ft"

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("HousePriceSolution").master("local[1]").getOrCreate()

    val realEstate = session.read.option("header", "true").csv("in/RealEstate.csv")

    val castedRealEstate = realEstate.withColumn(PRICE, realEstate(PRICE).cast("long"))
      .withColumn(PRICE_SQ_FT, realEstate(PRICE_SQ_FT).cast("long"))

    castedRealEstate.groupBy("Location")
      .avg(PRICE_SQ_FT)
      .orderBy("avg(Price SQ Ft)")
      .show()
  }
}
