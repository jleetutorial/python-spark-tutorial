package com.sparkTutorial.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TypedDataset {

  val AGE_MIDPOINT = "age_midpoint"
  val SALARY_MIDPOINT = "salary_midpoint"
  val SALARY_MIDPOINT_BUCKET = "salaryMidpointBucket"

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[*]").getOrCreate()
    val dataFrameReader = session.read

    val responses = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/2016-stack-overflow-survey-responses.csv")

    val responseWithSelectedColumns = responses.select("country", "age_midpoint", "occupation", "salary_midpoint")

    import session.implicits._
    val typedDataset = responseWithSelectedColumns.as[Response]

    System.out.println("=== Print out schema ===")
    typedDataset.printSchema()

    System.out.println("=== Print 20 records of responses table ===")
    typedDataset.show(20)

    System.out.println("=== Print the responses from Afghanistan ===")
    typedDataset.filter(response => response.country == "Afghanistan").show()

    System.out.println("=== Print the count of occupations ===")
    typedDataset.groupBy(typedDataset.col("occupation")).count().show()

    System.out.println("=== Print responses with average mid age less than 20 ===")
    typedDataset.filter(response => response.age_midpoint.isDefined && response.age_midpoint.get < 20.0).show()

    System.out.println("=== Print the result by salary middle point in descending order ===")
    typedDataset.orderBy(typedDataset.col(SALARY_MIDPOINT).desc).show()

    System.out.println("=== Group by country and aggregate by average salary middle point ===")
    typedDataset.filter(response => response.salary_midpoint.isDefined).groupBy("country").avg(SALARY_MIDPOINT).show()

    System.out.println("=== Group by salary bucket ===")
    typedDataset.map(response => response.salary_midpoint.map(point => Math.round(point / 20000) * 20000).orElse(None))
      .withColumnRenamed("value", SALARY_MIDPOINT_BUCKET)
      .groupBy(SALARY_MIDPOINT_BUCKET)
      .count()
      .orderBy(SALARY_MIDPOINT_BUCKET).show()
  }
}
