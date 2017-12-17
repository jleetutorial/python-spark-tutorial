from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("reduce").setMaster("local[*]")
    sc = SparkContext(conf = conf)
   
    inputIntegers = [1, 2, 3, 4, 5]
    integerRdd = sc.parallelize(inputIntegers)
   
    product = integerRdd.reduce(lambda x, y: x * y)
    print("product is :{}".format(product))
