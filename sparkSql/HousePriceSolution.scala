package com.sparkTutorial.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object HousePriceSolution {

  val PRICE_SQ_FT = "Price SQ Ft"

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("HousePriceSolution").master("local[1]").getOrCreate()

    val realEstate = session.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/RealEstate.csv")

    realEstate.groupBy("Location")
      .avg(PRICE_SQ_FT)
      .orderBy("avg(Price SQ Ft)")
      .show()
  }
}
