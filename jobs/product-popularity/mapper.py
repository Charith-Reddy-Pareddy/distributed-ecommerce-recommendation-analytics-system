#!/usr/bin/env python3
"""MapReduce mapper: reads raw event JSON lines from HDFS (one per
line, as hdfs-sink writes them) and emits (product_id, weight) pairs.
Hadoop Streaming feeds this process stdin/stdout -- no Hadoop-specific
API needed, just read lines, write tab-separated key\\tvalue pairs.
"""
import json
import sys

EVENT_WEIGHTS = {"view": 1, "add_to_cart": 3, "purchase": 5}


def main() -> None:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue

        product_id = event.get("product_id")
        weight = EVENT_WEIGHTS.get(event.get("event_type"))
        if product_id is None or weight is None:
            continue

        print(f"{product_id}\t{weight}")


if __name__ == "__main__":
    main()
