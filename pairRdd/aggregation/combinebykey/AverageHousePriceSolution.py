from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("AverageHousePrice").setMaster("local")
    sc = SparkContext(conf = conf)

    lines = sc.textFile("in/RealEstate.csv")
    cleanedLines = lines.filter(lambda line: "Bedrooms" not in line)

    housePricePairRdd = cleanedLines.map(lambda line: (line.split(",")[3], float(line.split(",")[2])))

    createCombiner = lambda x: (1, x)
    mergeValue = lambda avgCount, x: (avgCount[0] + 1, avgCount[1] + x)
    mergeCombiners = lambda avgCountA, avgCountB: (avgCountA[0] + avgCountB[0], avgCountA[1] + avgCountB[1])

    housePriceTotal = housePricePairRdd.combineByKey(createCombiner, mergeValue, mergeCombiners)

    housePriceAvg = housePriceTotal.mapValues(lambda avgCount: avgCount[1] / avgCount[0])
    for bedrooms, avgPrice in housePriceAvg.collect():
        print("{} : {}".format(bedrooms, avgPrice))
