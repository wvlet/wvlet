# Code Block Examples

Here's a simple code block:

```
SELECT * FROM users
```

Here's a code block with language hint:

```sql
SELECT id, name, email
FROM users
WHERE status = 'active'
ORDER BY created_at DESC
```

Another example with different language:

```scala
case class User(id: Int, name: String, email: String)

def findUser(id: Int): Option[User] =
  users.find(_.id == id)
```
