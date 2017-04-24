#!/bin/bash

INPUT_FILE="/u/home/mikegoss/COMPx705Public/data/Lab4Short/tiny_graph.csv"

OUTPUT_DIR="$PWD/out_local"

# Please assign the start vertex ID here:
SOURCE_VERTEXID="4"

rm -rf "$OUTPUT_DIR"

spark-submit --class "Lab4" --master local[*] target/scala-2.11/lab4-playing-with-graphx_2.11-0.1.0.jar 'local[*]' "file://$INPUT_FILE" "file://$OUTPUT_DIR" "$SOURCE_VERTEXID"


echo "checking results..."
DIFFERENCE="$(diff -U 0 <(cat out_local/part-0000* | sort) <(cat ~mikegoss/COMPx705Public/Labs/Lab4SampleOutputShort$SOURCE_VERTEXID/part-0000* | sort) | grep ^@ | wc -l)"
echo -e "Lines Different From Real Results: ${DIFFERENCE}"
