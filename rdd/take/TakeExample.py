import sys
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext("local", "take")
    sc.setLogLevel("ERROR")
    inputWords = ["spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop"]
    wordRdd = sc.parallelize(inputWords)
    words = wordRdd.take(3)
    for word in words:
        print(word)
