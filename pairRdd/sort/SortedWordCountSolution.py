from pyspark import SparkContext

if __name__ == "__main__":

    sc = SparkContext("local", "wordCounts")
    sc.setLogLevel("ERROR")
    lines = sc.textFile("in/word_count.text")
    wordRdd = lines.flatMap(lambda line: line.split(" "))

    wordPairRdd = wordRdd.map(lambda word: (word, 1))
    wordToCountPairs = wordPairRdd.reduceByKey(lambda x, y: x + y)

    countToWordParis = wordToCountPairs.map(lambda wordToCount: (wordToCount[1], wordToCount[0]))

    sortedCountToWordParis = countToWordParis.sortByKey(ascending=False)

    sortedWordToCountPairs = sortedCountToWordParis.map(lambda countToWord: (countToWord[1], countToWord[0]))

    for word, count in  sortedWordToCountPairs.collect():
        print("{} : {}".format(word, count))
