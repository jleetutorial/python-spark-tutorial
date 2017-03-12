package com.sparkTutorial.advanced.broadcast;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class UkMakerSpacesWithoutBroadcast {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("UkMakerSpaces").setMaster("local[1]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        final Map<String, String> postCodeMap = loadPostCodeMap();

        JavaRDD<String> makerSpaceRdd = javaSparkContext.textFile("in/uk-makerspaces-identifiable-data.csv");

        JavaRDD<String> regions = makerSpaceRdd
                .filter(line -> !line.split(Utils.COMMA_DELIMITER, -1)[0].equals("Timestamp"))
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
        String[] splits = line.split(Utils.COMMA_DELIMITER, -1);
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
            String[] splits = line.split(Utils.COMMA_DELIMITER, -1);
            postCodeMap.put(splits[0], splits[7]);
        }
        return  postCodeMap;
    }

}
