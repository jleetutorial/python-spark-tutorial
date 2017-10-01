from pyspark import SparkContext
from commons.Utils import Utils

def getPostPrefix(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    postcode = splits[4]
    return None if not postcode else postcode.split(" ")[0]

def loadPostCodeMap():
    lines = open("in/uk-postcode.csv", "r").read().split("\n")
    splitsForLines = [Utils.COMMA_DELIMITER.split(line) for line in lines if line != ""]
    return {splits[0]: splits[7] for splits in splitsForLines}

if __name__ == "__main__":
    sc = SparkContext("local", "UkMakerSpaces")
    sc.setLogLevel("ERROR")

    postCodeMap = sc.broadcast(loadPostCodeMap())

    makerSpaceRdd = sc.textFile("in/uk-makerspaces-identifiable-data.csv")

    regions = makerSpaceRdd \
      .filter(lambda line: Utils.COMMA_DELIMITER.split(line)[0] != "Timestamp") \
      .filter(lambda line: getPostPrefix(line) is not None) \
      .map(lambda line: postCodeMap.value[getPostPrefix(line)] \
        if getPostPrefix(line) in postCodeMap.value else "Unknow")

    for region, count in regions.countByValue().items():
        print("{} : {}".format(region, count))
