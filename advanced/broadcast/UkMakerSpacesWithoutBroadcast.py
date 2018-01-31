import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from commons.Utils import Utils

def loadPostCodeMap():
    lines = open("in/uk-postcode.csv", "r").read().split("\n")
    splitsForLines = [Utils.COMMA_DELIMITER.split(line) for line in lines if line != ""]
    return {splits[0]: splits[7] for splits in splitsForLines}

def getPostPrefix(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    postcode = splits[4]
    return None if not postcode else postcode.split(" ")[0]

if __name__ == "__main__":
    conf = SparkConf().setAppName('UkMakerSpaces').setMaster("local[*]")
    sc = SparkContext(conf = conf)
    postCodeMap = loadPostCodeMap()
    makerSpaceRdd = sc.textFile("in/uk-makerspaces-identifiable-data.csv")

    regions = makerSpaceRdd \
      .filter(lambda line: Utils.COMMA_DELIMITER.split(line)[0] != "Timestamp") \
      .map(lambda line: postCodeMap[getPostPrefix(line)] \
        if getPostPrefix(line) in postCodeMap else "Unknow")

    for region, count in regions.countByValue().items():
        print("{} : {}".format(region, count))
