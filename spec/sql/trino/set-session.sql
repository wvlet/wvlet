-- Basic session property
SET SESSION distributed_join = 'true';

-- Catalog-specific session property
SET SESSION example.incremental_refresh_enabled = false;

-- Another catalog property
SET SESSION acc01.optimize_locality_enabled = false;

-- String values
SET SESSION query_max_run_time = '2h';

-- Numeric values
SET SESSION memory_limit = 100;

-- Boolean values
SET SESSION experimental_features_enabled = true;