# Dashboard data sources

This note records the real source fields available for dashboard analytics and the identifier boundaries to preserve.

## Amazon Reviews 2023

Source: [McAuley Lab dataset](https://amazon-reviews-2023.github.io/).

The product importer reads `asin`, `name`, `category`, `brand`, `price`, `description`, `image`, `average_rating`, and `rating_number`. The ALS review reader expects `user_id`, `asin`, `rating`, and `timestamp`. In the inspected Desktop backup, the real review CSV had those four columns; its product extract contained 7,675 ASINs and the ASIN-to-integer catalog map had 7,675 unique integer IDs.

`asin` is the Amazon source product key. The integer `product_id` is this project's mapped catalog key. Amazon review `user_id` values are strings.

## RetailRocket

Source: [RetailRocket e-commerce dataset](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset).

The source `events.csv` uses `timestamp`, `visitorid`, `event`, `itemid`, and `transactionid`. Its item-property files use `timestamp`, `itemid`, `property`, and `value`; the category tree uses `categoryid` and `parentid`. The source event values include `view`, `addtocart`, and `transaction`.

These IDs belong to RetailRocket. `visitorid` is not an Amazon review `user_id`, and `itemid` is not an ASIN or this project's mapped `product_id`. The inspected current checkout and backup data folder did not contain RetailRocket files, so RetailRocket-backed charts must remain unavailable until the real source files are supplied.

## Dashboard rules

- Keep Amazon reviews, RetailRocket events, and application events in distinct source namespaces unless a verified crosswalk is provided.
- Build Amazon review charts from real review rows and RetailRocket interaction charts only from real RetailRocket event rows.
- The current Cassandra event API reads recent events for one user; it does not provide global event aggregates for dashboard charts.
- Show an unavailable or empty state when the required real data or aggregate endpoint is missing. Do not fill charts with sample or fabricated values.
