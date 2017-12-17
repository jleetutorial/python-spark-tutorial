from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName('GroupByKeyVsReduceByKey').setMaster("local[*]") 
    sc = SparkContext(conf = conf)

    words = ["one", "two", "two", "three", "three", "three"]
    wordsPairRdd = sc.parallelize(words).map(lambda word: (word, 1))

    wordCountsWithReduceByKey = wordsPairRdd \
        .reduceByKey(lambda x, y: x + y) \
        .collect()
    print("wordCountsWithReduceByKey: {}".format(list(wordCountsWithReduceByKey)))

    wordCountsWithGroupByKey = wordsPairRdd \
        .groupByKey() \
        .mapValues(len) \
        .collect()
    print("wordCountsWithGroupByKey: {}".format(list(wordCountsWithGroupByKey)))


