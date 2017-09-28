from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext("local", "sameHosts")

    julyFirstLogs = sc.textFile("in/nasa_19950701.tsv")
    augustFirstLogs = sc.textFile("in/nasa_19950801.tsv")
    
    julyFirstHosts = julyFirstLogs.map(lambda line: line.split("\t")[0])
    augustFirstHosts = augustFirstLogs.map(lambda line: line.split("\t")[0])
    
    intersection = julyFirstHosts.intersection(augustFirstHosts)
    
    cleanedHostIntersection = intersection.filter(lambda host: host != "host")
    cleanedHostIntersection.saveAsTextFile("out/nasa_logs_same_hosts.csv")
