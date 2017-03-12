package com.sparkTutorial.sparkSql.join;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class UkMakerSpaces {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder().appName("UkMakerSpaces").master("local[*]").getOrCreate();

        Dataset<Row> makerSpace = session.read().option("header", "true").csv("in/uk-makerspaces-identifiable-data.csv");

        Dataset<Row> postCode = session.read().option("header", "true").csv("in/uk-postcode.csv")
                .withColumn("PostCode", concat_ws("", col("PostCode"), lit(" ")));

        System.out.println("=== Print 20 records of makerspace table ===");
        makerSpace.show();

        System.out.println("=== Print 20 records of postcode table ===");
        postCode.show();

        Dataset<Row> joined = makerSpace.join(postCode,
                makerSpace.col("Postcode").startsWith(postCode.col("Postcode")), "left_outer");

        System.out.println("=== Group by Region ===");
        joined.groupBy("Region").count().show(200);
    }
}