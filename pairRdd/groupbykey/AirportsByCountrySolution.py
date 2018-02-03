import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from commons.Utils import Utils

if __name__ == "__main__":

    conf = SparkConf().setAppName("airports").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    lines = sc.textFile("in/airports.text")

    countryAndAirportNameAndPair = lines.map(lambda airport:\
         (Utils.COMMA_DELIMITER.split(airport)[3],
          Utils.COMMA_DELIMITER.split(airport)[1]))

    airportsByCountry = countryAndAirportNameAndPair.groupByKey()

    for country, airportName in airportsByCountry.collectAsMap().items():
        print("{}: {}".format(country, list(airportName)))
