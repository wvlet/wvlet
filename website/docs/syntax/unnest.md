
# Unnest

The `unnest` function is used to expand an array into table rows:

```wvlet
from unnest([1, 2, 3]) as t(number)
```

```
┌────────┐
│ number │
│  int   │
├────────┤
│      1 │
│      2 │
│      3 │
├────────┤
│ 3 rows │
└────────┘
```

If used with other columns, `unnest` will expand the array into rows and duplicate the other columns:
```wvlet
select unnest([1, 2, 3]), 10
```

```
┌──────────────────────────┬─────┐
│ unnest((ARRAY[1, 2, 3])) │ 10  │
│           int            │ int │
├──────────────────────────┼─────┤
│                        1 │  10 │
│                        2 │  10 │
│                        3 │  10 │
├──────────────────────────┴─────┤
│ 3 rows                         │
└────────────────────────────────┘
```

Unnest can be used to expand an array column as individual rows if it is used with `cross join`:

```wvlet
from [
  [''John'', [7, 10, 9]],
  [''Mary'', [4, 8, 9]],
] as tests(student, scores)
cross join unnest(scores) as t(score)
select student, score
```

```
┌─────────┬───────┐
│ student │ score │
│ string  │  int  │
├─────────┼───────┤
│ John    │     7 │
│ John    │    10 │
│ John    │     9 │
│ Mary    │     4 │
│ Mary    │     8 │
│ Mary    │     9 │
├─────────┴───────┤
│ 6 rows          │
└─────────────────┘
```

