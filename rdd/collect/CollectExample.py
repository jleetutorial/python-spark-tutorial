from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext("local", "collect")
    sc.setLogLevel("ERROR")
    inputWords = ["spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop"]
    wordRdd = sc.parallelize(inputWords)
    words = wordRdd.collect()
    for word in words:
        print(word)