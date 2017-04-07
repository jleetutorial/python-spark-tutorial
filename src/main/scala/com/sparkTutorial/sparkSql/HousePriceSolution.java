package com.sparkTutorial.sparkSql;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

public class HousePriceSolution {

    private static final String PRICE = "Price";
    private static final String PRICE_SQ_FT = "Price SQ Ft";

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("HousePriceSolution").master("local[1]").getOrCreate();

        Dataset<Row> realEstate = session.read().option("header", "true").csv("in/RealEstate.csv");

        Dataset<Row> castedRealEstate = realEstate.withColumn(PRICE, col(PRICE).cast("long"))
                                                  .withColumn(PRICE_SQ_FT, col(PRICE_SQ_FT).cast("long"));

        castedRealEstate.groupBy("Location")
                        .agg(avg(PRICE_SQ_FT), max(PRICE))
                        .orderBy(col("avg(" + PRICE_SQ_FT + ")").desc())
                        .show();
    }
}
