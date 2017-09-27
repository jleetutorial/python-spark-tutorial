from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext("local", "reduce")
    inputIntegers = [1, 2, 3, 4, 5]
    integerRdd = sc.parallelize(inputIntegers)
    product = integerRdd.reduce(lambda x, y: x * y)
    print("product is :{}".format(product))
