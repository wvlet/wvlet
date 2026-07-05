# Connector - Trino

## Connecting to Trino

A profile describes a working environment and can activate multiple connectors at once. Each entry in `connectors` is a named connection; the one marked `"default": true` (or the first one) is the engine your queries run on.

The profile file is parsed as JSONC, so `// line comments`, `/* block comments */`, and trailing commas are allowed.

```jsonc title='~/.wvlet/profiles.json'
{
  "profiles": [
    {
      "name": "trino",
      "connectors": [
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
      "connectors": [
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
  ]
}
```

```bash
$ wv --profile td
wv> 
```
