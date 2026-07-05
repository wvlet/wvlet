# Connector - Slack

The Slack connector exposes a Slack workspace as queryable tables and lets flows post messages
back to channels. It is a *source* connector: it has no SQL engine of its own, so its tables are
staged automatically into the active engine (e.g. DuckDB) when a query references them.

## Configuration

Create a [Slack app](https://api.slack.com/apps) with a bot token that has the
`channels:read`, `channels:history`, `users:read`, and `chat:write` scopes, then add a
connector to your profile:

```jsonc title='~/.wvlet/profiles.json'
{
  "profiles": [
    {
      "name": "dev",
      "connectors": [
        { "name": "duckdb", "type": "duckdb", "default": true, "catalog": "memory", "schema": "main" },
        { "name": "slack", "type": "slack", "properties": { "token": "${SLACK_TOKEN}" } }
      ]
    }
  ]
}
```

Optional properties:

- **message_fetch_limit**: messages fetched per channel when scanning the `messages` table
  (default 1000)

## Tables

| Table | Columns |
|-------|---------|
| `channels` | `id`, `name`, `is_private`, `num_members`, `topic` |
| `users` | `id`, `name`, `real_name`, `is_bot` |
| `messages` | `channel`, `user`, `text`, `ts`, `thread_ts` |

```sql
-- Channels with the most members
from slack.channels
| order by num_members desc
| limit 10

-- Message counts per channel
from slack.messages
| group by channel
| agg count(*) as messages
```

Scanning `messages` reads the recent history of every channel the bot can access, so prefer
filtering early and consider a lower `message_fetch_limit` for large workspaces.

## Posting messages from flows

Every profile connector with tools is an activation target under its instance name. The
`post_message` tool posts a stage's output (or an explicit `text:`) to a channel:

```sql
flow daily_report = {
  stage summary = from slack.messages | group by channel | agg count(*) as messages
  stage notify = from summary
    | activate('slack', tool: 'post_message', channel: '#reports')
}
```

When `text:` is omitted, the first rows of the stage output are attached as JSON lines.

## Posting messages ad hoc

Outside of a flow, the `call` statement invokes a tool directly:

```sql
call slack.post_message(channel: '#reports', text: 'hello from wvlet')
```

See [Working with Multiple Connectors](connectors.md#calling-connector-tools) for details on
`call`.
