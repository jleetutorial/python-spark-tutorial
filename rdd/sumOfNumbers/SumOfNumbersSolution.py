from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("primeNumbers").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    lines = sc.textFile("in/prime_nums.text")
    numbers = lines.flatMap(lambda line: line.split("\t"))

    validNumbers = numbers.filter(lambda number: number)
    
    intNumbers = validNumbers.map(lambda number: int(number))
    
    print("Sum is: {}".format(intNumbers.reduce(lambda x, y: x + y)))

