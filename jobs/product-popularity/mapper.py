#!/usr/bin/env python3
"""MapReduce mapper: reads raw event JSON lines from HDFS and emits
(product_id, weight) pairs over stdout as tab-separated key/value
pairs, per Hadoop Streaming's protocol.
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
