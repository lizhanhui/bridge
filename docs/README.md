# Task SQL notes

Task SQL in `conf/tasks.json` is [PartiQL](https://partiql.org/), evaluated against the
incoming record's JSON payload bound to the global name `payload`:

```sql
SELECT * FROM payload WHERE payload.age < 30
```

## Gotchas

### Reserved keywords in field names

PartiQL reserved keywords (e.g. `VALUE`, `SELECT`, `WHERE`, ...) cannot be used as
bare field references. A payload field named `value` must be double-quoted:

```sql
-- fails: "no viable alternative at input 'r.value'"
SELECT * FROM payload.readings AS r WHERE r.value > 10

-- works
SELECT * FROM payload.readings AS r WHERE r."value" > 10
```

### Multiple output rows per record

A query may produce zero, one, or many rows from a single record (e.g. by scanning an
array field). Each row becomes a separate output message; zero rows means the record
is filtered out.

```sql
-- one output message per array element (bare values, not wrapped in {"t": ...})
SELECT VALUE t FROM payload.tags AS t
```
