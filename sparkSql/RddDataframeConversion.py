import sys
sys.path.insert(0, '.')
from pyspark.sql import SparkSession
from commons.Utils import Utils

def mapResponseRdd(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    double1 = None if not splits[6] else float(splits[6])
    double2 = None if not splits[14] else float(splits[14])
    return splits[2], double1, splits[9], double2

def getColNames(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return [splits[2], splits[6], splits[9], splits[14]]

if __name__ == "__main__":

    session = SparkSession.builder.appName("StackOverFlowSurvey").master("local[*]").getOrCreate()
    sc = session.sparkContext

    lines = sc.textFile("in/2016-stack-overflow-survey-responses.csv")

    responseRDD = lines \
        .filter(lambda line: not Utils.COMMA_DELIMITER.split(line)[2] == "country") \
        .map(mapResponseRdd)    

    colNames = lines \
        .filter(lambda line: Utils.COMMA_DELIMITER.split(line)[2] == "country") \
        .map(getColNames)

    responseDataFrame = responseRDD.toDF(colNames.collect()[0])

    print("=== Print out schema ===")
    responseDataFrame.printSchema()

    print("=== Print 20 records of responses table ===")
    responseDataFrame.show(20)

    for response in responseDataFrame.rdd.take(10):
        print(response)