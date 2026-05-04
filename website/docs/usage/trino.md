# Connector - Trino

## Connecting to Trino

The profile file is parsed as JSONC, so `// line comments`, `/* block comments */`, and trailing commas are allowed.

```jsonc title='~/.wvlet/profiles.json'
{
  "profiles": [
    {
      "name": "trino",
      "type": "trino",
      "user": "(user name)",
      "password": "(password)",
      "host": "(trino host name, e.g., localhost:8080)",
      "port": 443,
      "catalog": "(your Trino catalog name)",
      "schema": "(your database)"
    }
  ]
}
```

```bash
$ wv --profile trino 
wv>
```


## Connecting to Trino at Treasure Data 

```jsonc title='~/.wvlet/profiles.json'
{
  "profiles": [
    {
      "name": "td",
      "type": "trino",
      "user": "${TD_API_KEY}",
      "password": "dummy",
      "host": "api-presto.treasuredata.com",
      "port": 443,
      "catalog": "td",
      "schema": "(your database name)"
    }
  ]
}
```

```bash
$ wv --profile td
wv> 
```
