from pyspark import SparkContext
from commons.Utils import Utils

def splitComma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[6])

if __name__ == "__main__":
    sc = SparkContext("local", "airports")
    
    airports = sc.textFile("in/airports.text")

    airportsInUSA = airports.filter(lambda line: float(Utils.COMMA_DELIMITER.split(line)[6]) > 40)
    
    airportsNameAndCityNames = airportsInUSA.map(splitComma)

    airportsNameAndCityNames.saveAsTextFile("out/airports_by_latitude.text")