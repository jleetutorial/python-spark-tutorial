from pyspark import SparkContext
from commons.Utils import Utils

if __name__ == "__main__":

    sc = SparkContext("local", "airports")
    sc.setLogLevel("ERROR")

    airportsRDD = sc.textFile("in/airports.text")

    airportPairRDD = airportsRDD.map(lambda line: \
        (Utils.COMMA_DELIMITER.split(line)[1], \
      Utils.COMMA_DELIMITER.split(line)[3]))

    upperCase = airportPairRDD.mapValues(lambda countryName: countryName.upper())

    upperCase.saveAsTextFile("out/airports_uppercase.text")
