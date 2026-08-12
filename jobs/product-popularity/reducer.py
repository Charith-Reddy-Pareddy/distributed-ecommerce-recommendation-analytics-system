#!/usr/bin/env python3
"""MapReduce reducer: sums weights per product_id.

Relies on Hadoop Streaming's guarantee that shuffle delivers all
records for a key contiguously and sorted, so a simple
compare-to-previous-key accumulation works without an in-memory dict.
"""
import sys


def main() -> None:
    current_product = None
    current_total = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        product_id, weight = line.split("\t", 1)
        weight = int(weight)

        if product_id == current_product:
            current_total += weight
        else:
            if current_product is not None:
                print(f"{current_product}\t{current_total}")
            current_product = product_id
            current_total = weight

    if current_product is not None:
        print(f"{current_product}\t{current_total}")


if __name__ == "__main__":
    main()
