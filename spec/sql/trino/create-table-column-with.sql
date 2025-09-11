-- Test file for Trino CREATE TABLE with column-level WITH properties

-- Basic column WITH clause (the failing case from the original error)
CREATE TABLE IF NOT EXISTS d_2185c.t_da91a (
   f_a0c96 bigint WITH ( key_name = 'Record arrival time' ),
   f_5ea28 varchar WITH ( key_name = 'beam_id' ),
   f_8a4c6 varchar WITH ( key_name = 'Combination of beam_id and brand house' ),
   f_d5ad3 varchar WITH ( key_name = 'category_name' ),
   f_8413a bigint WITH ( key_name = 'score' ),
   f_9d304 bigint WITH ( key_name = 'time' )
);

-- Simple test with single column
CREATE TABLE test_simple (
   id bigint WITH ( description = 'Primary key field' )
);

-- Column WITH clause combined with NOT NULL
CREATE TABLE test_not_null (
   id bigint NOT NULL WITH ( description = 'Primary key field' )
);

-- Multiple properties in single WITH clause
CREATE TABLE test_multiple_props (
   data_field varchar WITH ( 
     key_name = 'Important Data',
     data_type = 'sensitive',
     encryption = 'AES-256'
   )
);