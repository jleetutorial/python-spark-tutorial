package com.sparkTutorial.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object StackOverFlowSurvey {

  val AGE_MIDPOINT = "age_midpoint"
  val SALARY_MIDPOINT = "salary_midpoint"
  val SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket"

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()

    val dataFrameReader = session.read

    val responses = dataFrameReader.option("header", "true").csv("in/2016-stack-overflow-survey-responses.csv")

    System.out.println("=== Print out schema ===")
    responses.printSchema()

    System.out.println("=== Print 20 records of responses table ===")
    responses.show(20)

    System.out.println("=== Print the so_region and self_identification columns of gender table ===")
    responses.select("so_region", "self_identification").show()

    System.out.println("=== Print records where the response is from Afghanistan ===")
    responses.filter(responses.col("country").===("Afghanistan")).show()

    System.out.println("=== Print the count of occupations ===")
    val groupedDataset = responses.groupBy("occupation")
    groupedDataset.count().show()

    System.out.println("=== Cast the salary mid point and age mid point to integer ===")
    val castedResponse = responses.withColumn(SALARY_MIDPOINT, responses.col(SALARY_MIDPOINT).cast("integer"))
      .withColumn(AGE_MIDPOINT, responses.col(AGE_MIDPOINT).cast("integer"))

    System.out.println("=== Print out casted schema ===")
    castedResponse.printSchema()

    import session.implicits._
    System.out.println("=== Print records with average mid age less than 20 ===")
    castedResponse.filter($"age_midpoint" < 20).show()

    System.out.println("=== Print the result by salary middle point in descending order ===")
    castedResponse.orderBy(castedResponse.col(SALARY_MIDPOINT).desc).show()

    System.out.println("=== Group by country and aggregate by average salary middle point and max age middle point ===")
    val datasetGroupByCountry = castedResponse.groupBy("country")
    datasetGroupByCountry.avg(SALARY_MIDPOINT).show()


    val responseWithSalaryBucket = castedResponse.withColumn(
      SALARY_MIDPOINT_BUCKET, castedResponse.col(SALARY_MIDPOINT).divide(20000).cast("integer").multiply(20000))

    System.out.println("=== With salary bucket column ===")
    responseWithSalaryBucket.select(SALARY_MIDPOINT, SALARY_MIDPOINT_BUCKET).show()

    System.out.println("=== Group by salary bucket ===")
    responseWithSalaryBucket.groupBy(SALARY_MIDPOINT_BUCKET).count().orderBy(SALARY_MIDPOINT_BUCKET).show()

    session.stop()
  }
}
