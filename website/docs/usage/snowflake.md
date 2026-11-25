# Connector - Snowflake

Wvlet supports connecting to Snowflake databases via JDBC. On the JVM CLI, the Snowflake connector is in the full package `wvlet-cli-full-(version).tar.gz`; see [Installation](install.md#jvm-cli).

## Connecting to Snowflake

To connect to Snowflake, create a profile in `~/.wvlet/profiles.json`. The file is parsed as JSONC, so `// line comments`, `/* block comments */`, and trailing commas are allowed.

```jsonc title='~/.wvlet/profiles.json'
{
  "profiles": [
    {
      "name": "snowflake",
      "connectors": [
        {
          "name": "snowflake",
          "type": "snowflake",
          "user": "(your Snowflake user name)",
          "host": "(your account identifier, e.g., myorg-myaccount)",
          "catalog": "(your database name)",
          "schema": "(your schema name, e.g., PUBLIC)",
          "properties": {
            // Key-pair authentication; see below for a programmatic access token
            "private_key_file": "/path/to/rsa_key.p8",
            "warehouse": "(your warehouse name)",
            "role": "(your role name, optional)"
          }
        }
      ]
    }
  ]
}
```

Then connect using the profile:

```bash
$ wv --profile snowflake
wv>
```

## Configuration Parameters

- **host**: Your Snowflake account identifier (e.g., `myorg-myaccount`). The connector will automatically append `.snowflakecomputing.com`.
- **user**: Your Snowflake username
- Authentication, one of the following. Snowflake requires MFA for people who sign in with a password and rejects passwords for service users, so use a key pair or a token for the connector:
  - **properties.private_key_file**: Path to a PKCS#8 private key (`.p8`) whose public key is assigned to the user ([key-pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth)). Set **properties.private_key_pwd** when the key is encrypted.
  - **properties.token**: A [programmatic access token](https://docs.snowflake.com/en/user-guide/programmatic-access-tokens).
  - **password**: A password, for accounts that still accept one.
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
