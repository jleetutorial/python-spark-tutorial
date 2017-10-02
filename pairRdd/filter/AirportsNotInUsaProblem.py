from pyspark import SparkContext

if __name__ == "__main__":

    '''
    Create a Spark program to read the airport data from in/airports.text;
    generate a pair RDD with airport name being the key and country name being the value.
    Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located,
    IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:

    ("Kamloops", "Canada")
    ("Wewak Intl", "Papua New Guinea")
    ...

    '''
