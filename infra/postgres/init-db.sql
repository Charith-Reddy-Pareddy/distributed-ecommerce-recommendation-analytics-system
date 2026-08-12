-- Each microservice owns its own database (database-per-service pattern).
-- event-service has no database of its own: Kafka is its durable log.
-- product-service has no database of its own either: it uses MongoDB
-- (see the mongo service in docker-compose.yml), not Postgres.
CREATE DATABASE user_db;
CREATE DATABASE analytics_db;
-- Owned by serving-optimizer: an audit log of autonomous index/partition/
-- refresh-interval decisions, kept separate from the databases it tunes.
CREATE DATABASE optimizer_db;
