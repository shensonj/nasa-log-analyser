# nasa-log-analyser
## Description
This is a Spark application that downloads the dataset at ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz and uses Apache Spark to determine the top-n most frequent visitors and urls for each day of the trace.

spark-submit --class src.scala.nasa_log_parser --master spark://spark:7077 top_n_processor-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
input \#input dir where the data is downloaded
output_data \ #dir table name by which the output of the program will be stored
spark://spark:7077 \ #spark master
5 \ #number of partition 
10 # this is the N in topN. Say if N=10 then for each day we will have top 3 visitors displayed
