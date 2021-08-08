#!/bin/bash
#
# Spark submit command
#
spark-submit --class org.log_analyser.AccessLogProcessor --master spark://spark:7077 top_n_processor-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
input \
output_data \
spark://spark:7077 \
5 \
3

echo "Run Completed :) !!"
echo "Moment of Truth: Yes its time for validation!!"