# Connector - Trino

## Connecting to Trino

```yaml title='~/.wvlet/profiles.yml'
profiles:
  - name: trino
    type: trino
    user: (user name)
    password: (password)
    host: (trino host name, e.g., localhost:8080)
    port: 443
    catalog: (your Trino catalog name)
    schema: (your database)
```

```bash
$ wv --profile trino 
wv>
```


## Connecting to Trino at Treasure Data 

```yaml title='~/.wvlet/profiles.yml'
profiles:
  - name: td
    type: trino
    user: $TD_API_KEY
    password: dummy
    host: api-presto.treasuredata.com
    port: 443
    catalog: td
    schema: (your database name)
```

```bash
$ wv --profile td
wv> 
```
