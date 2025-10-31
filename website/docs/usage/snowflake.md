# Connector - Snowflake

Wvlet supports connecting to Snowflake databases via JDBC.

## Connecting to Snowflake

To connect to Snowflake, create a profile in `~/.wvlet/profiles.yml`:

```yaml title='~/.wvlet/profiles.yml'
profiles:
  - name: snowflake
    type: snowflake
    user: (your Snowflake user name)
    password: (your password)
    host: (your account identifier, e.g., myorg-myaccount)
    catalog: (your database name)
    schema: (your schema name, e.g., PUBLIC)
    properties:
      warehouse: (your warehouse name)
      role: (your role name, optional)
```

Then connect using the profile:

```bash
$ wv --profile snowflake
wv>
```

## Configuration Parameters

- **host**: Your Snowflake account identifier (e.g., `myorg-myaccount`). The connector will automatically append `.snowflakecomputing.com`.
- **user**: Your Snowflake username
- **password**: Your Snowflake password
- **catalog**: The database name to connect to
- **schema**: The schema name (default: `PUBLIC`)
- **properties.warehouse**: (Optional) The virtual warehouse to use
- **properties.role**: (Optional) The role to use for the session

## Example Usage

```sql
wv> from information_schema.tables
    where table_schema = 'PUBLIC'
    select table_name, table_type
```

## Notes

- Snowflake uses `ARRAY_CONSTRUCT()` for array creation and `OBJECT_CONSTRUCT()` for object/map creation
- Wvlet will automatically translate standard SQL constructs to Snowflake-compatible syntax
- Ensure your Snowflake account has the appropriate permissions and network access configured
