#!/bin/bash


# Set names of files for input and output.
#  Input file is already on HDFS
#  Output directory will be copied back from HDFS
INPUT_FILE='hdfs:///Public/data/Lab4Full/large_graph_a?'
OUTPUT_DIR=out_yarn

# Please Specify Source VertexID here
SOURCE_VERTEXID="1"
# Create HDFS directory path same as current directory
hadoop fs -mkdir -p "hdfs://$PWD"

FILE=""
# Remove any old copies of output directory on HDFS and Linux FS
hadoop fs -rm -f -r "hdfs://$PWD/$OUTPUT_DIR"
rm -rf "./$OUTPUT_DIR"

# Run Spark word count job

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --name "Lab4999" \
    --num-executors 4 \
    --class Lab4 \
    target/scala-2.11/lab4-playing-with-graphx_2.11-0.1.0.jar yarn "$INPUT_FILE" "hdfs://$PWD/$OUTPUT_DIR" "$SOURCE_VERTEXID"

# Copy result from HDFS to Linux FS
echo "Copying output from $OUTPUT_DIR"
hadoop fs -get "hdfs://$PWD/$OUTPUT_DIR" .

echo "Done! Checking Results..."


DIFFERENCE="$(diff -U 0 <(cat $OUTPUT_DIR/part-0000* | sort) <(cat FILE | sort) | grep ^@ | wc -l)"
echo -e "Lines Different From Real Results: ${DIFFERENCE}"
