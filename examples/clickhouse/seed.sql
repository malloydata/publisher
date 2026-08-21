-- Seed data for the `clickhouse` example package. Run automatically by
-- docker-compose on first start.
CREATE DATABASE IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.orders (
   order_id UInt64,
   customer_id UInt32,
   order_date Date,
   status Enum8('pending' = 1, 'shipped' = 2, 'delivered' = 3, 'cancelled' = 4),
   region LowCardinality(String),
   item_count UInt8,
   amount Decimal(10, 2),
   discount Nullable(Float64)
) ENGINE = MergeTree ORDER BY (order_date, order_id);

INSERT INTO demo.orders
SELECT
   number + 1,
   toUInt32(1 + (number * 7919) % 500),
   toDate('2025-01-01') + toIntervalDay((number * 13) % 400),
   CAST(1 + (number % 4) AS Enum8('pending' = 1, 'shipped' = 2, 'delivered' = 3, 'cancelled' = 4)),
   ['north', 'south', 'east', 'west', 'central'][1 + (number % 5)],
   toUInt8(1 + (number % 9)),
   toDecimal64(round(10 + (number * 17 % 4900) / 10.0, 2), 2),
   if(number % 6 = 0, NULL, round((number % 30) / 100.0, 4))
FROM numbers(50000);
