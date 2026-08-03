-- Each microservice owns its own database (database-per-service pattern).
-- event-service has no database of its own: Kafka is its durable log.
CREATE DATABASE product_db;
CREATE DATABASE user_db;
CREATE DATABASE analytics_db;
