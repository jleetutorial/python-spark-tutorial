from pyspark import SparkContext
from commons.Utils import Utils

def getPostPrefixes(line: str):
    postcode = Utils.COMMA_DELIMITER.split(line)[4]
    cleanedPostCode = postcode.replace("\\s+", "")
    return [cleanedPostCode[0:i] for i in range(0,len(cleanedPostCode)+1)]

def loadPostCodeMap():
    lines = open("in/uk-postcode.csv", "r").read().split("\n")
    splitsForLines = [Utils.COMMA_DELIMITER.split(line) for line in lines if line != ""]
    return {splits[0]: splits[7] for splits in splitsForLines}

if __name__ == "__main__":
    sc = SparkContext("local", "UkMakerSpaces")
    sc.setLogLevel("ERROR")
    postCodeMap = loadPostCodeMap()
    makerSpaceRdd = sc.textFile("in/uk-makerspaces-identifiable-data.csv")

    regions = makerSpaceRdd \
      .filter(lambda line: Utils.COMMA_DELIMITER.split(line)[0] != "Timestamp") \
      .map(lambda line: next((postCodeMap[prefix] for prefix in getPostPrefixes(line) \
      if prefix in postCodeMap), "Unknow"))

    for region, count in regions.countByValue().items():
        print("{} : {}".format(region, count))
