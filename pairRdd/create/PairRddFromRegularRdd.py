from pyspark import SparkContext

if __name__ == "__main__":

    sc = SparkContext("local", "create")
    sc.setLogLevel("ERROR")

    inputStrings = ["Lily 23", "Jack 29", "Mary 29", "James 8"]
    regularRDDs = sc.parallelize(inputStrings)

    pairRDD = regularRDDs.map(lambda s: (s.split(" ")[0], s.split(" ")[1]))
    pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_regular_rdd")
