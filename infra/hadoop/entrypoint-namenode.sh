#!/usr/bin/env bash
set -euo pipefail

# Only format on the very first run -- formatting an already-initialized
# namenode directory would wipe its metadata (and disagree with datanodes
# that already registered against the old cluster ID).
if [ ! -d /hadoop/dfs/name/current ]; then
  echo "Formatting namenode (first run)..."
  hdfs namenode -format -force -nonInteractive
fi

exec hdfs namenode
