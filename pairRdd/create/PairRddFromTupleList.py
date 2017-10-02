from pyspark import SparkContext

if __name__ == "__main__":

    sc = SparkContext("local", "create")
    sc.setLogLevel("ERROR")

    tuples = [("Lily", 23), ("Jack", 29), ("Mary", 29), ("James", 8)]
    pairRDD = sc.parallelize(tuples)

    pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_tuple_list")
