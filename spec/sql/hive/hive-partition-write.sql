-- Hive partition write options: CLUSTER BY, DISTRIBUTE BY, SORT BY

-- INSERT with CLUSTER BY
INSERT INTO TABLE sales_partitioned
SELECT * FROM sales
CLUSTER BY region, year;

-- INSERT with DISTRIBUTE BY and SORT BY
INSERT INTO TABLE sales_partitioned
SELECT * FROM sales
DISTRIBUTE BY region
SORT BY year DESC, month ASC;

-- INSERT OVERWRITE with CLUSTER BY
INSERT OVERWRITE TABLE sales_partitioned
SELECT * FROM sales
CLUSTER BY customer_id;

-- Multiple partition write options
INSERT INTO TABLE orders_partitioned
SELECT * FROM orders
DISTRIBUTE BY customer_id, order_date
SORT BY order_amount DESC;

-- CREATE TABLE AS with CLUSTER BY
CREATE TABLE clustered_sales AS
SELECT * FROM sales
CLUSTER BY region, product_id;

-- CREATE TABLE AS with DISTRIBUTE BY and SORT BY
CREATE TABLE distributed_sales AS
SELECT * FROM sales
DISTRIBUTE BY region
SORT BY sales_amount DESC;

-- Complex query with CLUSTER BY
INSERT INTO TABLE analytics_table
SELECT 
    region,
    product_id,
    SUM(sales_amount) as total_sales,
    COUNT(*) as transaction_count
FROM sales
WHERE year = 2024
GROUP BY region, product_id
CLUSTER BY region;

-- WITH clause and CLUSTER BY
WITH regional_sales AS (
    SELECT region, SUM(amount) as total
    FROM sales
    GROUP BY region
)
INSERT INTO TABLE summary_table
SELECT * FROM regional_sales
CLUSTER BY region;

-- INSERT with column list and DISTRIBUTE BY
INSERT INTO TABLE target_table (col1, col2, col3)
SELECT a, b, c FROM source_table
DISTRIBUTE BY col1
SORT BY col2;

-- SORT BY with multiple columns and different orderings
INSERT INTO TABLE sorted_data
SELECT * FROM raw_data
SORT BY priority DESC, timestamp ASC, id;

-- All three options together
INSERT INTO TABLE optimized_table
SELECT * FROM source_data
DISTRIBUTE BY partition_key
SORT BY sort_key1 ASC, sort_key2 DESC
CLUSTER BY cluster_key;

-- CREATE TABLE IF NOT EXISTS with CLUSTER BY
CREATE TABLE IF NOT EXISTS partitioned_table AS
SELECT * FROM source
CLUSTER BY date_column, category;

-- CREATE OR REPLACE TABLE with DISTRIBUTE BY
CREATE OR REPLACE TABLE distributed_table AS
SELECT * FROM source
DISTRIBUTE BY hash_key
SORT BY timestamp DESC;