INPUT_FILE="graph2.txt"
OUTPUT_FILE="the_output"
START_NODE="A"
rm -rf iteration* $OUTPUT_FILE
javac -cp ".:Spark-Core.jar" AssigTwoz5015906.java 
java -cp ".:Spark-Core.jar" AssigTwoz5015906 $START_NODE $INPUT_FILE $OUTPUT_FILE 
