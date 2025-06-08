---
id: catalog-management
title: Catalog Management
---

# Catalog Management

Wvlet's Static Catalog feature allows you to export database metadata (schemas, tables, functions) and use it for offline query compilation. This is particularly useful for:

- **CI/CD pipelines**: Compile and validate queries without database access
- **Offline development**: Work on queries without network connectivity
- **Performance**: Avoid repeated catalog API calls during compilation
- **Stability**: Use a consistent catalog snapshot for testing

## Quick Start

### 1. Import Catalog from Database

Export your database catalog to local JSON files:

```bash
# Import from DuckDB (default)
wvlet catalog import --name mydb

# Import from Trino
wvlet catalog import --type trino --name prod_catalog --profile mytrino

# Import specific schema only
wvlet catalog import --name mydb --schema sales

# Custom catalog directory
wvlet catalog import --path ./my-catalogs --name mydb
```

### 2. List Available Catalogs

View all imported catalogs:

```bash
wvlet catalog list

# With custom path
wvlet catalog list --path ./my-catalogs
```

Output:
```
Available static catalogs:
  duckdb/mydb
  trino/prod_catalog
```

### 3. Show Catalog Details

Inspect a specific catalog:

```bash
wvlet catalog show duckdb/mydb
```

Output:
```
Catalog: mydb (DuckDB)
Schemas: 3
  main: 5 tables
  sales: 12 tables
  analytics: 8 tables
Functions: 2650
```

### 4. Compile with Static Catalog

Use the imported catalog for offline compilation:

```bash
# Compile using static catalog
wvlet compile -f query.wv --use-static-catalog --catalog mydb

# Specify custom catalog path
wvlet compile -f query.wv --use-static-catalog --static-catalog-path ./my-catalogs --catalog mydb
```

## Catalog Structure

Static catalogs are stored as JSON files in the following structure:

```
./catalog/                    # Default catalog directory
├── duckdb/                  # Database type
│   └── mydb/               # Catalog name
│       ├── schemas.json    # List of schemas
│       ├── main.json       # Tables in 'main' schema
│       ├── sales.json      # Tables in 'sales' schema
│       └── functions.json  # SQL functions
└── trino/
    └── prod_catalog/
        └── ...
```

## Common Workflows

### Development Workflow

1. **Initial Setup**: Import catalog from development database
   ```bash
   wvlet catalog import --name dev_db
   ```

2. **Write Queries**: Develop queries with auto-completion and validation
   ```wv
   -- queries/customer_analysis.wv
   from sales.customers c
   join sales.orders o on c.customer_id = o.customer_id
   where o.created_at > date '2024-01-01'
   select c.name, count(*) as order_count
   group by c.name
   ```

3. **Compile Offline**: Validate queries without database connection
   ```bash
   wvlet compile -f queries/customer_analysis.wv --use-static-catalog --catalog dev_db
   ```

### CI/CD Workflow

1. **Store Catalog in Version Control**: Commit catalog files
   ```bash
   git add catalog/
   git commit -m "Update database catalog snapshot"
   ```

2. **CI Pipeline**: Compile and validate all queries
   ```yaml
   # .github/workflows/validate-queries.yml
   - name: Validate Queries
     run: |
       find queries -name "*.wv" -exec \
         wvlet compile -f {} --use-static-catalog --catalog prod_db \;
   ```

### Team Collaboration

1. **Shared Catalog**: One team member exports the catalog
   ```bash
   wvlet catalog import --name shared_db --path ./shared-catalog
   ```

2. **Distribution**: Share via git, S3, or other storage
   ```bash
   aws s3 sync ./shared-catalog s3://team-bucket/catalogs/
   ```

3. **Team Usage**: Others download and use the catalog
   ```bash
   aws s3 sync s3://team-bucket/catalogs/ ./catalog/
   wvlet compile -f query.wv --use-static-catalog --catalog shared_db
   ```

## Keeping Catalogs Updated

### Manual Refresh

Refresh catalog when schema changes:

```bash
wvlet catalog refresh --name mydb
```

This is equivalent to re-importing:
```bash
wvlet catalog import --name mydb
```

### Automated Updates

Set up a scheduled job to keep catalogs current:

```bash
#!/bin/bash
# update-catalog.sh
wvlet catalog import --name prod_db --profile production
git add catalog/
git commit -m "Auto-update catalog $(date +%Y-%m-%d)"
git push
```

## Best Practices

1. **Version Control**: Store catalog files in git for tracking changes
   ```
   catalog/
   ├── .gitignore  # Exclude large/temporary files
   └── README.md   # Document catalog sources and update procedures
   ```

2. **Naming Conventions**: Use descriptive catalog names
   - Environment: `dev_db`, `staging_db`, `prod_db`
   - Purpose: `analytics_db`, `reporting_db`
   - Version: `sales_db_v2`, `catalog_2024_06`

3. **Security**: Don't store sensitive metadata
   - Review exported schemas before committing
   - Exclude internal/system schemas if needed
   - Use `.gitignore` for sensitive catalogs

4. **Performance**: For large catalogs
   - Import only required schemas
   - Consider splitting by domain/team
   - Use selective imports with `--schema` flag

## Troubleshooting

### Common Issues

1. **"Catalog not found" error**
   - Check catalog path: `wvlet catalog list --path ./catalog`
   - Verify catalog name matches exactly
   - Ensure JSON files exist in catalog directory

2. **"Table not found" during compilation**
   - Refresh catalog if schema changed: `wvlet catalog refresh --name mydb`
   - Check if table exists in correct schema
   - Verify catalog was imported from correct database

3. **Large catalog files**
   - Import specific schemas: `--schema sales`
   - Exclude system schemas
   - Consider splitting into multiple catalogs

### Debug Commands

```bash
# Check what's in the catalog
wvlet catalog show duckdb/mydb

# Verify catalog file structure
ls -la catalog/duckdb/mydb/

# Test compilation with verbose logging
wvlet compile -f query.wv --use-static-catalog --catalog mydb --debug
```

## Configuration

### Environment Variables

```bash
# Default catalog directory
export WVLET_STATIC_CATALOG_PATH=/path/to/catalogs

# Default database type
export WVLET_DB_TYPE=trino
```

### Configuration File

```yaml
# .wvlet/config.yml
static_catalog:
  path: ./catalog
  default_catalog: prod_db
  auto_refresh: false
```

## See Also

- [CLI Reference](./cli.md) - Complete CLI command reference
- [Static Catalog Architecture](../development/static-catalog.md) - Technical implementation details
- [DuckDB Usage](./duckdb.md) - DuckDB-specific features
- [Trino Usage](./trino.md) - Trino-specific features
