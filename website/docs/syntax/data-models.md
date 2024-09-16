# Data Models

:::warning
This page is still work in progress.
:::

### Defining Data Models 

In Wvlet, you can define reusable data models, which wraps an Wvlet query with `model (model name) = ... end` block:

```sql
model my_model =
  -- Write your query here
  from ...
  ... 
end
```

Models can be used in other queries in the same manner with scanning a table:

```sql
from my_model
limit 10
```

Data models are often the units to __materialize query results into the target database tables__. If your data model needs to be accessed by multiple queries, materializing (or persisting) data models will reduce the cost of data processing and often accelerates the query processing.   
