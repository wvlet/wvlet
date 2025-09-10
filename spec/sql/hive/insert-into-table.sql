-- Test INSERT INTO TABLE with various table name formats including backquoted identifiers

-- Basic INSERT INTO TABLE
INSERT INTO TABLE customers
SELECT * FROM source_table;

-- INSERT INTO TABLE with backquoted identifier
INSERT INTO TABLE `cdp_tmp_customers`
SELECT * FROM source_table;

-- INSERT INTO TABLE with schema and backquoted table name
INSERT INTO TABLE schema1.`table_name`
SELECT * FROM source_table;

-- INSERT INTO TABLE with all backquoted identifiers
INSERT INTO TABLE `schema`.`table`
SELECT * FROM source_table;

-- INSERT INTO TABLE with column list
INSERT INTO TABLE `customers` (id, name, email)
SELECT id, name, email FROM source_table;

-- Complex Hive query from the error example
INSERT INTO TABLE `cdp_tmp_customers`
select coalesce(cast(conv(substr(cdp_customer_id,1,2),16,10) as bigint)*3600 div 32,0) as time, * from (
  select
    sha1(concat_ws('.', cast(time as string), cast(row_number() over (partition by time) as string))) as cdp_customer_id,
    m.`user`,
    m.`host`,
    m.`path`,
    m.`referer`,
    m.`code`,
    m.`agent`,
    m.`size`,
    m.`method`
  from `sample_datasets`.`www_access` m
) d;

-- INSERT INTO without TABLE keyword (standard SQL)
INSERT INTO customers
SELECT * FROM source_table;

-- INSERT INTO with backquoted identifier without TABLE keyword
INSERT INTO `cdp_tmp_customers`
SELECT * FROM source_table;

-- INSERT OVERWRITE TABLE (standard Hive syntax)
INSERT OVERWRITE TABLE customers
SELECT * FROM source_table;

-- INSERT OVERWRITE TABLE with backquoted identifier
INSERT OVERWRITE TABLE `cdp_tmp_customers`
SELECT * FROM source_table;

-- INSERT OVERWRITE TABLE with schema and backquoted table name
INSERT OVERWRITE TABLE schema1.`table_name`
SELECT * FROM source_table;
