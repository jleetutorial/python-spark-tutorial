package com.sparkTutorial.rdd.commons

object Utils {
 // a regular expression which matches commas but not commas within double quotations
    val COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
}
