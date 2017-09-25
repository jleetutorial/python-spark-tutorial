import sys
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext("local", "primeNumbers")
    lines = sc.textFile("in/prime_nums.text")
    numbers = lines.flatMap(lambda line: line.split("\t"))
    validNumbers = numbers.filter(lambda number: number)
    intNumbers = validNumbers.map(lambda number: int(number))
    print("Sum is: ")
    print(intNumbers.reduce(lambda x, y: x + y))