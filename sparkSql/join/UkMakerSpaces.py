from pyspark.sql import SparkSession, functions as fs

if __name__ == "__main__":
    session = SparkSession.builder.appName("UkMakerSpaces").master("local[*]").getOrCreate()

    makerSpace = session.read.option("header", "true") \
        .csv("in/uk-makerspaces-identifiable-data.csv")

    postCode = session.read.option("header", "true").csv("in/uk-postcode.csv") \
        .withColumn("PostCode", fs.concat_ws("", fs.col("PostCode"), fs.lit(" ")))

    print("=== Print 20 records of makerspace table ===")
    makerSpace.select("Name of makerspace", "Postcode").show()

    print("=== Print 20 records of postcode table ===")
    postCode.select("PostCode", "Region").show()

    joined = makerSpace \
        .join(postCode, makerSpace["Postcode"].startswith(postCode["Postcode"]), "left_outer")

    print("=== Group by Region ===")
    joined.groupBy("Region").count().show(200)