"""Build a descriptive report from the real Amazon Reviews 2023 CSV."""

import argparse
import csv
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


def summarize_reviews(path: str | Path) -> dict[str, object]:
    """Aggregate review ratings and product counts without retaining user IDs."""
    review_path = Path(path)
    ratings: Counter[float] = Counter()
    product_counts: Counter[str] = Counter()
    product_sums: dict[str, float] = {}
    total = 0
    rating_sum = 0.0
    first_ts: int | None = None
    last_ts: int | None = None

    with review_path.open(newline="", encoding="utf-8") as stream:
        rows = csv.DictReader(stream)
        required = {"asin", "rating", "timestamp"}
        if not rows.fieldnames or not required.issubset(rows.fieldnames):
            raise ValueError("Amazon reviews must include asin, rating, and timestamp")

        for line, row in enumerate(rows, start=2):
            asin = row["asin"].strip()
            try:
                rating = float(row["rating"])
                timestamp = int(row["timestamp"])
            except (TypeError, ValueError) as exc:
                raise ValueError(f"invalid rating or timestamp on CSV line {line}") from exc
            if not asin or not 1 <= rating <= 5 or timestamp <= 0:
                raise ValueError(f"invalid review fields on CSV line {line}")

            ratings[rating] += 1
            product_counts[asin] += 1
            product_sums[asin] = product_sums.get(asin, 0.0) + rating
            rating_sum += rating
            total += 1
            first_ts = timestamp if first_ts is None else min(first_ts, timestamp)
            last_ts = timestamp if last_ts is None else max(last_ts, timestamp)

    if not total or first_ts is None or last_ts is None:
        raise ValueError("Amazon review CSV contains no review rows")

    counts = sorted(product_counts.values())
    middle = len(counts) // 2
    median = (
        float(counts[middle])
        if len(counts) % 2
        else (counts[middle - 1] + counts[middle]) / 2
    )
    p90 = counts[(9 * len(counts) + 9) // 10 - 1]

    def as_utc(value: int) -> str:
        # Amazon Reviews 2023 stores its timestamp field as Unix milliseconds.
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()

    return {
        "source_file": review_path.name,
        "review_rows": total,
        "reviewed_products": len(product_counts),
        "timestamp_range_utc": [as_utc(first_ts), as_utc(last_ts)],
        "weighted_average_rating": round(rating_sum / total, 4),
        "mean_product_average_rating": round(
            sum(product_sums[item] / product_counts[item] for item in product_counts)
            / len(product_counts),
            4,
        ),
        "rating_counts": {str(value): count for value, count in sorted(ratings.items())},
        "reviews_per_product": {
            "min": counts[0],
            "median": median,
            "p90": p90,
            "max": counts[-1],
            "single_review_products": sum(count == 1 for count in counts),
        },
    }


def render_report(summary: dict[str, object]) -> str:
    dist = summary["rating_counts"]
    product = summary["reviews_per_product"]
    dates = summary["timestamp_range_utc"]
    total = summary["review_rows"]
    lines = [
        "# Amazon review research report",
        "",
        "Descriptive results computed from the real Amazon Reviews 2023 extract. "
        "The source CSV is local and is not included in this repository.",
        "",
        f"- Source file: `{summary['source_file']}`",
        f"- Review rows: {total:,}",
        f"- Distinct reviewed ASINs: {summary['reviewed_products']:,}",
        f"- Review timestamp range (UTC): {dates[0]} to {dates[1]}",
        f"- Weighted mean rating: {summary['weighted_average_rating']:.4f}",
        f"- Unweighted mean of ASIN rating averages: {summary['mean_product_average_rating']:.4f}",
        "",
        "## Rating distribution",
        "",
        "| Rating | Reviews | Share |",
        "| ---: | ---: | ---: |",
    ]
    for rating, count in dist.items():
        lines.append(f"| {rating} | {count:,} | {count / total:.2%} |")
    lines.extend(
        [
            "",
            "## Reviews per product",
            "",
            f"The median is {product['median']:,.1f} reviews per ASIN; the 90th "
            f"percentile is {product['p90']:,}. Counts range from {product['min']:,} "
            f"to {product['max']:,}; {product['single_review_products']:,} ASINs "
            "have one review.",
            "",
            "## Interpretation and limits",
            "",
            "These are descriptive summaries of observed reviews, not estimates of "
            "sales or causal effects. Product averages have unequal review counts, "
            "so they should be read alongside the volume distribution above.",
            "",
            "ALS recommendation quality has not been measured here. A separate "
            "time-ordered held-out evaluation against a popularity baseline is "
            "required before making a quality claim. RetailRocket and global "
            "application-event questions remain blocked as described in "
            "[the source notes](dashboard-data-sources.md).",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reviews", type=Path, required=True)
    parser.add_argument(
        "--output", type=Path, default=Path("docs/research_report.md")
    )
    args = parser.parse_args()
    report = render_report(summarize_reviews(args.reviews))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
