# Code Block with Title Attribute

Example of code block with title:

```wv title="/query.wv"
from 'sample.json'
```

Another example with SQL:

```sql title="user_query.sql"
SELECT id, name, email
FROM users
WHERE status = 'active'
```

Multiple attributes:

```python title="main.py" highlight="1,3-5"
def main():
    print("Hello")
    x = 1
    y = 2
    return x + y
```
