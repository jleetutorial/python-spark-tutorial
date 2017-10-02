from pyspark import SparkContext

if __name__ == "__main__":

    sc = SparkContext("local", "wordCounts")
    sc.setLogLevel("ERROR")

    lines = sc.textFile("in/word_count.text")
    wordRdd = lines.flatMap(lambda line: line.split(" "))
    wordPairRdd = wordRdd.map(lambda word: (word, 1))

    wordCounts = wordPairRdd.reduceByKey(lambda x, y: x + y)
    for word, count in wordCounts.collect():
        print("{} : {}".format(word, count))
