#!/usr/bin/env bash
set -euo pipefail

# Only format on first run -- re-formatting would wipe existing metadata.
if [ ! -d /hadoop/dfs/name/current ]; then
  echo "Formatting namenode (first run)..."
  hdfs namenode -format -force -nonInteractive
fi

exec hdfs namenode
