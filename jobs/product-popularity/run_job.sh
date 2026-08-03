#!/usr/bin/env bash
set -euo pipefail

INPUT_GLOB="/events/dt=*/*.jsonl"
OUTPUT_DIR="/output/product-popularity-$(date +%s)"
FINAL_FILE="/output/product-popularity-ranked.tsv"

echo "=== Waiting for namenode to leave safe mode ==="
hdfs dfsadmin -safemode wait

echo "=== Running Hadoop Streaming MapReduce job over $INPUT_GLOB ==="
hadoop jar "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.name="product-popularity" \
  -files /job/mapper.py,/job/reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input "$INPUT_GLOB" \
  -output "$OUTPUT_DIR"

echo
echo "=== Raw aggregated output (product_id, weighted score) ==="
hdfs dfs -cat "$OUTPUT_DIR"/part-* | tee /tmp/aggregated.tsv

echo
echo "=== Ranking by popularity score (descending) ==="
sort -t $'\t' -k2,2nr /tmp/aggregated.tsv > /tmp/ranked.tsv

hdfs dfs -rm -f "$FINAL_FILE" > /dev/null 2>&1 || true
hdfs dfs -put -f /tmp/ranked.tsv "$FINAL_FILE"

echo
echo "=== Final ranked output written to $FINAL_FILE ==="
hdfs dfs -cat "$FINAL_FILE"
