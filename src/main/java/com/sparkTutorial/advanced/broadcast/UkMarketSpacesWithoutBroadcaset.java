package com.sparkTutorial.advanced.broadcast;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class UkMarketSpacesWithoutBroadcaset {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("UkMarketSpaces").setMaster("local[1]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        final Map<String, String> postCodeMap = loadPostCodeMap();

        JavaRDD<String> marketsRdd = javaSparkContext.textFile("in/uk-market-spaces-identifiable-data.csv");

        JavaRDD<String> regions = marketsRdd
                .filter(line -> !line.split(",", -1)[0].equals("Timestamp"))
                .map(line -> {
                    List<String> postCodePrefixes = getPostPrefixes(line);
                    for (String  postCodePrefix: postCodePrefixes) {
                        if (postCodeMap.containsKey(postCodePrefix)) {
                            return postCodeMap.get(postCodePrefix);
                        }
                    }
                    return "Unknown";
                });
        for (Map.Entry<String, Long> regionCounts : regions.countByValue().entrySet()) {
            System.out.println(regionCounts.getKey() + " : " + regionCounts.getValue());
        }
    }

    private static List<String> getPostPrefixes(String line) {
        String[] splits = line.split(",", -1);
        String postcode = splits[4];
        String cleanedPostCode = postcode.replaceAll("\\s+", "");
        ArrayList<String> prefixes = new ArrayList<>();
        for (int i = 1; i <= cleanedPostCode.length(); i ++) {
            prefixes.add(cleanedPostCode.substring(0, i));
        }
        return prefixes;
    }

    private static Map<String, String> loadPostCodeMap() throws FileNotFoundException {
        Scanner postCode = new Scanner(new File("in/uk-postcode.csv"));
        Map<String, String> postCodeMap = new HashMap<>();
        while (postCode.hasNextLine()) {
            String line = postCode.nextLine();
            String[] splits = line.split(",", -1);
            postCodeMap.put(splits[0], splits[7]);
        }
        return  postCodeMap;
    }

}
