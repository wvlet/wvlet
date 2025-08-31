# Test Syntax

In wvlet, you can insert `test` statements in the middle of a query. For example, you can test the number of rows, column names, or the query result itself:

```json title='person.json'
[
  {"id":1, "name": "alice", "age": 10 },
  {"id":2, "name": "bob", "age": 24 },
  {"id":3, "name": "clark", "age": 40 }
]
```

```wvlet
from 'person.json'
-- Test the query size and schema
test _.size should be 3
test _.columns should be ['id', 'name', 'age']
test _.columns should contain 'name'
test _.columns should not contain 'address'

-- Test the query output. Surrounding white spaces will be trimmed:
test _.output should be """
┌──────┬────────┬──────┐
│  id  │  name  │ age  │
│ long │ string │ long │
├──────┼────────┼──────┤
│    1 │ alice  │   10 │
│    2 │ bob    │   24 │
│    3 │ clark  │   40 │
├──────┴────────┴──────┤
│ 3 rows               │
└──────────────────────┘
"""

-- Test the query result in JSON format
test _.json should be """
{"id":1,"name":"alice","age":10}
{"id":2,"name":"bob","age":24}
{"id":3,"name":"clark","age":40}
"""
test _.json should contain """{"id":2,"name":"bob","age":24}"""
test _.json should not contain """{"id":4,"name":"david","age":32}"""

-- Test rows with arrays
test _.rows should contain [1, "alice", 10]
test _.rows should contain [2, "bob", 24]
test _.rows should not contain [5, "eric", 18]
test _.rows should be [
  [1, "alice", 10],
  [2, "bob", 24],
  [3, "clark", 40],
]
```

These tests will be ignored in the default query execution. 
Only in the test-run mode, these test expressions will be evaluated.  

## Result Inspection Functions

| Syntax    | Output                                 |
|-----------|----------------------------------------|
| _.size    | The number of rows in the query result |
| _.columns | The column names in the query result   | 
| _.output  | Pretty print query result              |  
| _.json    | JSON line format of result rows        |
| _.rows    | rows in array of array representation  |


## Test Expressions

| Syntax    | Description                                  |
|-----------|----------------------------------------------|
| `x` should be `y` | Test if the value of `x` is equal to `y`     |  
| `x` should not be `y` | Test if the value of `x` is not equal to `y` |
| `x` should contain `y` | Test if the string value of `x` contains `y` |
| `x` should not contain `y` | Test if the string value of `x` does not contain `y` |
| `x` = `y` | Test if the value of `x` is equal to `y`     |
| `x` is `y` | Test if the value of `x` is equal to `y`     |
| `x` != `y` | Test if the value of `x` is not equal to `y` |
| `x` is not `y` | Test if the value of `x` is not equal to `y` |
| `x` `(cmp)` `y` | Compare `x` and `y` with `<`, `<=`, `=>`, `>` |
