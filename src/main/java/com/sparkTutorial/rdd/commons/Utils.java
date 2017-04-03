package com.sparkTutorial.rdd.commons;

public class Utils {
 // a regular expression which matches commas but not commas within double quotations
    public static String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
}
