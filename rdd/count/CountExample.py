from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext("local", "count")
    inputWords = ["spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop"]
    wordRdd = sc.parallelize(inputWords)
    print("Count: {}".format(wordRdd.count()))
    worldCountByValue = wordRdd.countByValue()
    print("CountByValue: ")
    for word, count in worldCountByValue.items():
        print("{} : {}".format(word, count))