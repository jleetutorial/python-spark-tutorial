package com.sparkTutorial.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.max;


public class TypedDataset {
    private static final String AGE_MIDPOINT = "ageMidpoint";
    private static final String SALARY_MIDPOINT = "salaryMidPoint";
    private static final String SALARY_MIDPOINT_BUCKET = "salaryMidpointBucket";
    private static final float NULL_VALUE = -1.0f;
    private static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate();

        JavaRDD<String> lines = sc.textFile("in/2016-stack-overflow-survey-responses.csv");

        JavaRDD<Response> responseRDD = lines
                .filter(line -> !line.split(COMMA_DELIMITER, -1)[2].equals("country"))
                .map(line -> {
                    String[] splits = line.split(COMMA_DELIMITER, -1);
                    return new Response(splits[2], convertStringToFloat(splits[6]), splits[9], convertStringToFloat(splits[14]));
                });
        Dataset<Response> responseDataset = session.createDataset(responseRDD.rdd(), Encoders.bean(Response.class));

        System.out.println("=== Print out schema ===");
        responseDataset.printSchema();

        System.out.println("=== Print 20 records of responses table ===");
        responseDataset.show(20);

        System.out.println("=== Print records where the response is from Afghanistan ===");
        responseDataset.filter(response -> response.getCountry().equals("Afghanistan")).show();

        System.out.println("=== Print the count of occupations ===");
        responseDataset.groupBy(responseDataset.col("occupation")).count().show();


        System.out.println("=== Print records with average mid age less than 20 ===");
        responseDataset.filter(response -> response.getAgeMidPoint() != NULL_VALUE && response.getAgeMidPoint() < 20).show();

        System.out.println("=== Print the result with salary middle point in descending order ===");
        responseDataset.orderBy(responseDataset.col(SALARY_MIDPOINT ).desc()).show();

        System.out.println("=== Group by country and aggregate by average salary middle point and max age middle point ===");
        responseDataset
                .filter(response -> response.getSalaryMidPoint() != NULL_VALUE)
                .groupBy("country")
                .agg(avg(SALARY_MIDPOINT), max(AGE_MIDPOINT))
                .show();

        System.out.println("=== Group by salary bucket ===");

        responseDataset
                .map(response -> Math.round(response.getSalaryMidPoint()/20000) * 20000, Encoders.INT())
                .withColumnRenamed("value", SALARY_MIDPOINT_BUCKET)
                .groupBy(SALARY_MIDPOINT_BUCKET)
                .count()
                .orderBy(SALARY_MIDPOINT_BUCKET).show();
    }

    private static float convertStringToFloat(String split) {
        return split.isEmpty() ? NULL_VALUE : Float.valueOf(split);
    }

}
