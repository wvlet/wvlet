# Testing Wvlet queries

`test` statements follow a query in a `.wv` file and assert on its result. They are evaluated in
test-run mode (spec runners and CI); a normal `wvlet run` ignores them.

```wvlet run
val users(id, name, age) = [
  [1, 'alice', 10],
  [2, 'bob', 24],
  [3, 'clark', 40],
]

from users
where age >= 18
select id, name
test _.size should be 2
test _.columns should be ['id', 'name']
test _.columns should contain 'name'
test _.columns should not contain 'age'
test _.rows should be [[2, 'bob'], [3, 'clark']]
test _.rows should contain [2, 'bob']
test _.rows should not contain [1, 'alice']
test _.json should contain """{"id":2,"name":"bob"}"""
```

## Result inspection

| Expression | Value |
|------------|-------|
| `_.size` | Number of rows |
| `_.columns` | Column names |
| `_.rows` | Rows as an array of arrays |
| `_.json` | Rows as JSON lines |
| `_.output` | The pretty-printed result table |

## Assertions

| Assertion | Meaning |
|-----------|---------|
| `x should be y`, `x = y`, `x is y` | Equal |
| `x should not be y`, `x != y`, `x is not y` | Not equal |
| `x should contain y` | Membership, or substring for strings |
| `x should not contain y` | Negated membership |
| `x < y`, `x <= y`, `x > y`, `x >= y` | Comparison, e.g. `test _.size > 0` |

## Tips

- Use inline tables (`val t(a, b) = [[...]]`) so a test does not depend on a database.
- Several `test` lines can follow one query; another query after them starts a new result.
- `_.output` compares the full box-drawn table; prefer `_.rows` or `_.json`, which do not
  depend on column widths.
- In the Wvlet repository, `.wv` files under `spec/` are test cases:
  `./sbt "runnerJVM/testOnly *RunnerSpecBasic -- spec:basic:<file>.wv"`.
