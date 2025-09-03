import gzip

# Create a simple gzipped JSON file with the same content as person.json
json_content = '''[
  {"id":1, "name": "alice", "age": 10 },
  {"id":2, "name": "bob", "age": 24 },
  {"id":3, "name": "clark", "age": 40 }
]'''

with gzip.open('/home/runner/work/wvlet/wvlet/spec/basic/person.json.gz', 'wb') as f:
    f.write(json_content.encode('utf-8'))

print("Created person.json.gz")