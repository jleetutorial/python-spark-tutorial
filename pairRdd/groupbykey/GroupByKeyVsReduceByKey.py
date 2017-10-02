from pyspark import SparkContext

if __name__ == "__main__":

    sc = SparkContext("local", "GroupByKeyVsReduceByKey")
    sc.setLogLevel("ERROR")

    words = ["one", "two", "two", "three", "three", "three"]
    wordsPairRdd = sc.parallelize(words).map(lambda word: (word, 1))

    wordCountsWithReduceByKey = wordsPairRdd.reduceByKey(lambda x, y: x + y).collect()
    print("wordCountsWithReduceByKey: {}".format(list(wordCountsWithReduceByKey)))

    wordCountsWithGroupByKey = wordsPairRdd \
        .groupByKey() \
        .mapValues(lambda intIterable: len(intIterable)) \
        .collect()
    print("wordCountsWithGroupByKey: {}".format(list(wordCountsWithGroupByKey)))
