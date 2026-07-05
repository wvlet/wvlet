# Working with Multiple Connectors

A profile in `~/.wvlet/profiles.json` describes a working environment and can activate several
connectors at once — for example a production Trino cluster next to a local DuckDB database:

```jsonc title='~/.wvlet/profiles.json'
{
  "profiles": [
    {
      "name": "dev",
      "connectors": [
        {
          "name": "td",
          "type": "trino",
          "default": true,
          "host": "api-presto.treasuredata.com",
          "user": "${TD_API_KEY}",
          "catalog": "td",
          "schema": "sample_datasets"
        },
        { "name": "local", "type": "duckdb", "catalog": "memory", "schema": "main" }
      ]
    }
  ]
}
```

The connector marked `"default": true` (or the first one) is the engine your queries run on when
the session starts.

## Referencing tables through a connector name

Table references can name a connector explicitly. The first identifier is checked against the
connector names of the active profile:

```sql
-- <connector>.<table>: uses the connector's configured schema
from td.www_access

-- <connector>.<schema>.<table>
from td.sample_datasets.www_access
```

Models, CTEs, and query aliases take precedence over connector names, and connector names take
precedence over schema names of the default catalog. Since you choose connector names yourself,
rename the connector if it collides with a schema you need to address.

Queries execute on one SQL engine at a time: referencing another *engine* connector reports an
error suggesting `use <connector>`. Tables of *source* connectors (services without a SQL
engine, like [Slack](slack.md)) are staged into the active engine automatically, so
`from slack.channels` works from any engine. Inside [flows](../syntax/flow.md),
cross-connector references are staged automatically too, and each stage can pick its engine
with `stage <name> on <connector>`. Cross-engine joins in ad-hoc queries are planned as a
follow-up.

## Switching connectors with `use`

The `use` statement switches the active connector for subsequent statements. Its catalog then
drives table resolution and the SQL dialect:

```sql
use td                  -- switch to the td connector
use local               -- switch to the local DuckDB connector
use connector td        -- explicit form, for when a name collides with a schema
use td.sample_datasets  -- switch connector and schema in one statement
```

Bare names are resolved connector-first: `use analytics` switches to a connector named
`analytics` when the profile defines one, and otherwise behaves like `use schema analytics`
(see [CLI reference](cli-reference.md) for the schema forms).
