from pyspark import SparkContext

if __name__ == "__main__":

    sc = SparkContext("local", "avgHousePrice")
    sc.setLogLevel("ERROR")

    lines = sc.textFile("in/RealEstate.csv")
    cleanedLines = lines.filter(lambda line: "Bedrooms" not in line)

    housePricePairRdd = cleanedLines.map(lambda line: \
        (line.split(",")[3], (1, float(line.split(",")[2]))))

    housePriceTotal = housePricePairRdd \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    print("housePriceTotal: ")
    for bedroom, total in housePriceTotal.collect():
        print("{} : {}".format(bedroom, total))

    housePriceAvg = housePriceTotal.mapValues(lambda avgCount: avgCount[1] / avgCount[0])
    print("\nhousePriceAvg: ")
    for bedroom, avg in housePriceAvg.collect():
        print("{} : {}".format(bedroom, avg))
