from pyspark.sql import SparkSession

AGE_MIDPOINT = "age_midpoint"
SALARY_MIDPOINT = "salary_midpoint"
SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket"

if __name__ == "__main__":

    session = SparkSession.builder.appName("StackOverFlowSurvey").master("local").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    dataFrameReader = session.read

    responses = dataFrameReader \
        .option("header", "true") \
        .option("inferSchema", value = True) \
        .csv("in/2016-stack-overflow-survey-responses.csv")
    
    print("=== Print out schema ===")
    responses.printSchema()
    
    responseWithSelectedColumns = responses.select("country", "occupation", AGE_MIDPOINT, SALARY_MIDPOINT)

    print("=== Print the selected columns of the table ===")
    responseWithSelectedColumns.show()

    print("=== Print records where the response is from Afghanistan ===")
    responseWithSelectedColumns.filter(responseWithSelectedColumns["country"] == "Afghanistan").show()

    print("=== Print the count of occupations ===")
    groupedDataset = responseWithSelectedColumns.groupBy("occupation")
    groupedDataset.count().show()

    print("=== Print records with average mid age less than 20 ===")
    responseWithSelectedColumns.filter(responseWithSelectedColumns[AGE_MIDPOINT] < 20).show()

    print("=== Print the result by salary middle point in descending order ===")
    responseWithSelectedColumns.orderBy(responseWithSelectedColumns[SALARY_MIDPOINT], ascending=False).show()

    print("=== Group by country and aggregate by average salary middle point ===")
    datasetGroupByCountry = responseWithSelectedColumns.groupBy("country")
    datasetGroupByCountry.avg(SALARY_MIDPOINT).show()

    responseWithSalaryBucket = responses.withColumn(SALARY_MIDPOINT_BUCKET,
        ((responses[SALARY_MIDPOINT]/20000).cast("integer")*20000))

    print("=== With salary bucket column ===")
    responseWithSalaryBucket.select(SALARY_MIDPOINT, SALARY_MIDPOINT_BUCKET).show()

    print("=== Group by salary bucket ===")
    responseWithSalaryBucket.groupBy(SALARY_MIDPOINT_BUCKET).count().orderBy(SALARY_MIDPOINT_BUCKET).show()

    session.stop()
