from pyspark import SparkContext

def isNotHeader(line: str):
    return not (line.startswith("host") and "bytes" in line)

if __name__ == "__main__":
    sc = SparkContext("local", "unionLogs")

    julyFirstLogs = sc.textFile("in/nasa_19950701.tsv")
    augustFirstLogs = sc.textFile("in/nasa_19950801.tsv")

    aggregatedLogLines = julyFirstLogs.union(augustFirstLogs)

    cleanLogLines = aggregatedLogLines.filter(isNotHeader)
    sample = cleanLogLines.sample(withReplacement = True, fraction = 0.1)

    sample.saveAsTextFile("out/sample_nasa_logs.csv")