from pyspark import SparkContext, StorageLevel

if __name__ == "__main__":
    sc = SparkContext("local", "persist")
    inputIntegers = [1, 2, 3, 4, 5]
    integerRdd = sc.parallelize(inputIntegers)
    integerRdd.persist(StorageLevel.MEMORY_ONLY)
    integerRdd.reduce(lambda x, y: x*y)
    integerRdd.count()
