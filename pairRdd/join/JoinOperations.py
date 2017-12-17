from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("JoinOperations").setMaster("local[1]")
    sc = SparkContext(conf = conf)
    
    ages = sc.parallelize([("Tom", 29), ("John", 22)])
    addresses = sc.parallelize([("James", "USA"), ("John", "UK")])

    join = ages.join(addresses)
    join.saveAsTextFile("out/age_address_join.text")

    leftOuterJoin = ages.leftOuterJoin(addresses)
    leftOuterJoin.saveAsTextFile("out/age_address_left_out_join.text")

    rightOuterJoin = ages.rightOuterJoin(addresses)
    rightOuterJoin.saveAsTextFile("out/age_address_right_out_join.text")

    fullOuterJoin = ages.fullOuterJoin(addresses)
    fullOuterJoin.saveAsTextFile("out/age_address_full_out_join.text")
