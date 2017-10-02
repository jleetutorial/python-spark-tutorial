from pairRdd.aggregation.reducebykey.housePrice.AvgCount import AvgCount
from pyspark import SparkContext


if __name__ == "__main__":

    sc = SparkContext("local", "averageHousePriceSolution")
    sc.setLogLevel("ERROR")

    lines = sc.textFile("in/RealEstate.csv")
    cleanedLines = lines.filter(lambda line: "Bedrooms" not in line)
    housePricePairRdd = cleanedLines.map(lambda line: \
    ((int(float(line.split(",")[3]))), AvgCount(1, float(line.split(",")[2]))))

    housePriceTotal = housePricePairRdd.reduceByKey(lambda x, y: \
        AvgCount(x.count + y.count, x.total + y.total))

    housePriceAvg = housePriceTotal.mapValues(lambda avgCount: avgCount.total / avgCount.count)

    sortedHousePriceAvg = housePriceAvg.sortByKey()

    for bedrooms, avgPrice in sortedHousePriceAvg.collect():
        print("{} : {}".format(bedrooms, avgPrice))
