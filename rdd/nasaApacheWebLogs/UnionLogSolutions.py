from pyspark import SparkContext, SparkConf

def isNotHeader(line: str):
    return not (line.startswith("host") and "bytes" in line)

if __name__ == "__main__":
    conf = SparkConf().setAppName("unionLogs").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    julyFirstLogs = sc.textFile("in/nasa_19950701.tsv")
    augustFirstLogs = sc.textFile("in/nasa_19950801.tsv")

    aggregatedLogLines = julyFirstLogs.union(augustFirstLogs)

    cleanLogLines = aggregatedLogLines.filter(isNotHeader)
    sample = cleanLogLines.sample(withReplacement = True, fraction = 0.1)

    sample.saveAsTextFile("out/sample_nasa_logs.csv")

