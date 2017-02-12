package com.sparkTutorial.sparkSql.join;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class UkMarketSpaces {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate();

        Dataset<Row> marketSpace = session.read().option("header", "true").csv("in/uk-market-spaces-identifiable-data.csv");
        Dataset<Row> postCode = session.read().option("header", "true").csv("in/uk-postcode.csv").withColumn("PostCode", concat_ws("", col("PostCode"), lit(" ")));

        postCode.show();
        marketSpace.show();

        Dataset<Row> joined = marketSpace.join(postCode, marketSpace.col("Postcode").startsWith(postCode.col("Postcode")));

        joined.show();

        joined.groupBy("Region").count().show(200);
    }
}