import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from commons.Utils import Utils

if __name__ == "__main__":
    conf = SparkConf().setAppName('StackOverFlowSurvey').setMaster("local[*]")
    sc = SparkContext(conf = conf)
    total = sc.accumulator(0)
    missingSalaryMidPoint = sc.accumulator(0)
    responseRDD = sc.textFile("in/2016-stack-overflow-survey-responses.csv")

    def filterResponseFromCanada(response):
        splits = Utils.COMMA_DELIMITER.split(response)
        total.add(1)
        if not splits[14]:
            missingSalaryMidPoint.add(1)
        return splits[2] == "Canada"

    responseFromCanada = responseRDD.filter(filterResponseFromCanada)
    print("Count of responses from Canada: {}".format(responseFromCanada.count()))
    print("Total count of responses: {}".format(total.value))
    print("Count of responses missing salary middle point: {}" \
        .format(missingSalaryMidPoint.value))
