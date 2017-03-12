package com.sparkTutorial.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import static com.sparkTutorial.rdd.commons.Utils.COMMA_DELIMITER;

public class RddDatasetConversion {

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
                    return new Response(splits[2], toInt(splits[6]), splits[9], toInt(splits[14]));
                });
        Dataset<Response> responseDataset = session.createDataset(responseRDD.rdd(), Encoders.bean(Response.class));

        System.out.println("=== Print out schema ===");
        responseDataset.printSchema();

        System.out.println("=== Print 20 records of responses table ===");
        responseDataset.show(20);

        JavaRDD<Response> responseJavaRDD = responseDataset.toJavaRDD();
        for (Response response : responseJavaRDD.collect()) {
            System.out.println(response);
        }
    }
    private static Integer toInt(String split) {
        return split.isEmpty() ? null : Math.round(Float.valueOf(split));
    }
}
