import sys
from pyspark import SparkContext

if __name__ == "__main__":
	sc = SparkContext("local", "word count")
  	lines = sc.textFile("in/word_count.text")
  	words = lines.flatMap(lambda line: line.split(" "))
  	wordCounts = words.countByValue()
  	for word, count in wordCounts.items():
  		print(word, count)