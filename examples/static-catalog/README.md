# Static Catalog Example

This example demonstrates how to use Wvlet's static catalog feature for offline query development and CI/CD pipelines.

## Setup

1. **Import catalog from your database:**

```bash
# For DuckDB
wv catalog import --name example_db

# For Trino
wv catalog import --type trino --name prod_catalog --profile production
```

2. **Verify the catalog was imported:**

```bash
wv catalog list
# Output: duckdb/example_db

wv catalog show duckdb/example_db
# Output: Catalog details with schemas and table counts
```

## Example Queries

### sales_summary.wv

```sql
-- Monthly sales summary
from sales.orders
where order_date >= date '2024-01-01'
group by 
  date_trunc('month', order_date) as month,
  region
select
  month,
  region,
  count(*) as order_count,
  sum(total_amount) as total_sales
order by month, region
```

### customer_analysis.wv

```sql
-- Top customers by revenue
from sales.customers c
join sales.orders o on c.customer_id = o.customer_id
where o.status = 'completed'
group by c.customer_id, c.name, c.country
select 
  c.name,
  c.country,
  count(o.order_id) as order_count,
  sum(o.total_amount) as lifetime_value
order by lifetime_value desc
limit 20
```

## Compile Queries Offline

```bash
# Compile with static catalog
wvlet compile -f sales_summary.wv --use-static-catalog --catalog example_db

# Compile all queries in the directory
for query in *.wv; do
  echo "Compiling: $query"
  wvlet compile -f "$query" --use-static-catalog --catalog example_db
done
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Validate Queries

on: [push, pull_request]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Install Wvlet
        run: |
          curl -L https://github.com/wvlet/wvlet/releases/latest/download/wvlet-linux-x64.tar.gz | tar xz
          sudo mv wvlet /usr/local/bin/
      
      - name: Validate All Queries
        run: |
          # Use the committed catalog
          find queries -name "*.wv" -exec \
            wvlet compile -f {} --use-static-catalog --catalog prod_db \;
```

### GitLab CI Example

```yaml
validate-queries:
  stage: test
  script:
    - wvlet compile -f queries/*.wv --use-static-catalog --catalog prod_db
  only:
    changes:
      - queries/**/*.wv
      - catalog/**/*.json
```

## Keeping Catalog Updated

### Manual Update Script

```bash
#!/bin/bash
# update-catalog.sh

echo "Updating catalog from production..."
wv catalog import --name prod_db --profile production

echo "Checking for changes..."
if git diff --quiet catalog/; then
  echo "No catalog changes detected"
else
  echo "Catalog changes detected, committing..."
  git add catalog/
  git commit -m "Update catalog snapshot $(date +%Y-%m-%d)"
  git push
fi
```

### Scheduled Update (cron)

```cron
# Update catalog every Monday at 2 AM
0 2 * * 1 cd /path/to/project && ./update-catalog.sh >> catalog-update.log 2>&1
```

## Best Practices

1. **Version Control**: Always commit catalog files with your queries
2. **Documentation**: Document which database/environment the catalog represents
3. **Regular Updates**: Schedule regular catalog updates to catch schema changes
4. **Testing**: Use static catalogs in CI to catch query errors early
5. **Team Sync**: Share catalog updates with team members promptly

## Troubleshooting

If queries fail to compile:

1. Check catalog is up to date: `wv catalog refresh --name example_db`
2. Verify table exists: `wv catalog show duckdb/example_db | grep table_name`
3. Check schema name is correct in the query
4. Enable debug logging: `wvlet compile -f query.wv --use-static-catalog --debug`