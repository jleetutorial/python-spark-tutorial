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

    val responses = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/2016-stack-overflow-survey-responses.csv")

    System.out.println("=== Print out schema ===")
    responses.printSchema()

    val responseWithSelectedColumns = responses.select("country", "occupation", AGE_MIDPOINT, SALARY_MIDPOINT)

    System.out.println("=== Print the selected columns of the table ===")
    responseWithSelectedColumns.show()

    System.out.println("=== Print records where the response is from Afghanistan ===")
    responseWithSelectedColumns.filter(responseWithSelectedColumns.col("country").===("Afghanistan")).show()

    System.out.println("=== Print the count of occupations ===")
    val groupedDataset = responseWithSelectedColumns.groupBy("occupation")
    groupedDataset.count().show()

    System.out.println("=== Print records with average mid age less than 20 ===")
    responseWithSelectedColumns.filter(responseWithSelectedColumns.col(AGE_MIDPOINT) < 20).show()

    System.out.println("=== Print the result by salary middle point in descending order ===")
    responseWithSelectedColumns.orderBy(responseWithSelectedColumns.col(SALARY_MIDPOINT).desc).show()

    System.out.println("=== Group by country and aggregate by average salary middle point ===")
    val datasetGroupByCountry = responseWithSelectedColumns.groupBy("country")
    datasetGroupByCountry.avg(SALARY_MIDPOINT).show()

    val responseWithSalaryBucket = responses.withColumn(SALARY_MIDPOINT_BUCKET,
      responses.col(SALARY_MIDPOINT).divide(20000).cast("integer").multiply(20000))

    System.out.println("=== With salary bucket column ===")
    responseWithSalaryBucket.select(SALARY_MIDPOINT, SALARY_MIDPOINT_BUCKET).show()

    System.out.println("=== Group by salary bucket ===")
    responseWithSalaryBucket.groupBy(SALARY_MIDPOINT_BUCKET).count().orderBy(SALARY_MIDPOINT_BUCKET).show()

    session.stop()
  }
}
