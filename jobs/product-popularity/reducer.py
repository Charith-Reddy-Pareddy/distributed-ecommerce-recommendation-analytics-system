#!/usr/bin/env python3
"""MapReduce reducer: sums weights per product_id.

Relies on the standard Hadoop Streaming guarantee that the shuffle
phase delivers all records for a given key contiguously and sorted --
that's what makes the simple "compare to previous key" accumulation
pattern below correct, rather than needing an in-memory dict keyed by
every product (which wouldn't scale to a real dataset).
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
