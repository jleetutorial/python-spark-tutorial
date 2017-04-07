package com.sparkTutorial.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

public class StackOverFlowSurvey {

    private static final String AGE_MIDPOINT = "age_midpoint";
    private static final String SALARY_MIDPOINT = "salary_midpoint";
    private static final String SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket";

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate();

        DataFrameReader dataFrameReader = session.read();

        Dataset<Row> responses = dataFrameReader.option("header","true").csv("in/2016-stack-overflow-survey-responses.csv");

        System.out.println("=== Print out schema ===");
        responses.printSchema();

        System.out.println("=== Print 20 records of responses table ===");
        responses.show(20);

        System.out.println("=== Print the so_region and self_identification columns of gender table ===");
        responses.select(col("so_region"),  col("self_identification")).show();

        System.out.println("=== Print records where the response is from Afghanistan ===");
        responses.filter(col("country").equalTo("Afghanistan")).show();

        System.out.println("=== Print the count of occupations ===");
        RelationalGroupedDataset groupedDataset = responses.groupBy(col("occupation"));
        groupedDataset.count().show();

        System.out.println("=== Cast the salary mid point and age mid point to integer ===");
        Dataset<Row> castedResponse = responses.withColumn(SALARY_MIDPOINT, col(SALARY_MIDPOINT).cast("integer"))
                                               .withColumn(AGE_MIDPOINT, col(AGE_MIDPOINT).cast("integer"));

        System.out.println("=== Print out casted schema ===");
        castedResponse.printSchema();

        System.out.println("=== Print records with average mid age less than 20 ===");
        castedResponse.filter(col(AGE_MIDPOINT).$less(20)).show();

        System.out.println("=== Print the result by salary middle point in descending order ===");
        castedResponse.orderBy(col(SALARY_MIDPOINT ).desc()).show();

        System.out.println("=== Group by country and aggregate by average salary middle point and max age middle point ===");
        RelationalGroupedDataset datasetGroupByCountry = castedResponse.groupBy("country");
        datasetGroupByCountry.agg(avg(SALARY_MIDPOINT), max(AGE_MIDPOINT)).show();


        Dataset<Row> responseWithSalaryBucket = castedResponse.withColumn(
                SALARY_MIDPOINT_BUCKET, col(SALARY_MIDPOINT).divide(20000).cast("integer").multiply(20000));

        System.out.println("=== With salary bucket column ===");
        responseWithSalaryBucket.select(col(SALARY_MIDPOINT), col(SALARY_MIDPOINT_BUCKET)).show();

        System.out.println("=== Group by salary bucket ===");
        responseWithSalaryBucket.groupBy(SALARY_MIDPOINT_BUCKET).count().orderBy(col(SALARY_MIDPOINT_BUCKET)).show();

        session.stop();
    }
}
