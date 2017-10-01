from pyspark import SparkContext
from commons.Utils import Utils

def filterResponseFromCanada(response, total, missingSalaryMidPoint, processedBytes):
    processedBytes.add(len(response.encode('utf-8')))
    splits = Utils.COMMA_DELIMITER.split(response)
    total.add(1)
    if not splits[14]:
        missingSalaryMidPoint.add(1)
    return splits[2] == "Canada"

if __name__ == "__main__":
    sc = SparkContext("local", "StackOverFlowSurvey")
    sc.setLogLevel("ERROR")

    total = sc.accumulator(0)
    missingSalaryMidPoint = sc.accumulator(0)
    processedBytes = sc.accumulator(0)

    responseRDD = sc.textFile("in/2016-stack-overflow-survey-responses.csv")

    responseFromCanada = responseRDD.filter(lambda response: \
        filterResponseFromCanada(response, total, missingSalaryMidPoint, processedBytes))

    print("Count of responses from Canada: {}".format(responseFromCanada.count()))
    print("Number of bytes processed: {}".format(processedBytes.value))
    print("Total count of responses: {}".format(total.value))
    print("Count of responses missing salary middle point: {}".format(missingSalaryMidPoint.value))
