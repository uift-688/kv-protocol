# KV Protocol Wrapper

This project provides a way to use **structured objects such as dictionaries and lists** on top of a **string-based Key-Value store**.

It is designed to be lightweight, extensible, and database-agnostic.

---

## Features

* ✅ Use **objects (dict, list, etc.)** on a string KV store
* ✅ **Nested structures** are fully supported
* ✅ Works with **any database** by inheriting `DatabaseBase`
* ✅ Simple and intuitive API

---

## Concept

Many KV databases only support `string -> string` operations.
This library wraps such databases and enables:

* Automatic serialization / deserialization
* Object-style access
* Nested data handling

All database implementations share the same interface by extending `DatabaseBase`.

---

## Usage

### 1. Implement a Database

Create your own database by inheriting `DatabaseBase`.

```python
from kv_protocol import DatabaseBase, DatabaseWrapper

class KV(DatabaseBase):
    def __init__(self):
        self.store = {}

    async def get(self, key: str) -> str:
        return self.store[key]

    async def set(self, key: str, value: str):
        self.store[key] = value

    async def delete(self, key: str):
        del self.store[key]

    async def exists(self, key: str) -> bool:
        return key in self.store
```

---

### 2. Wrap the Database

```python
db = DatabaseWrapper(KV())
```

---

### 3. Store and Access Objects

```python
db.set("foo", {"a": "b", "c": "d"})

data = db.get("foo")

print(data)          # {'a': 'b', 'c': 'd'}
print(data["a"])     # b
print(data.has("e")) # False
```

* Objects behave like dictionaries
* Nested access works naturally
* Utility methods like `.has()` are available

---

## Supported Data Types

* `dict`
* `list`
* `str`
* `int`, `float`, `bool`
* Nested combinations of the above

---

## Database Compatibility

Any database can be used as long as it implements:

* `get`
* `set`
* `delete`
* `exists`

This includes:

* In-memory stores
* Redis-like databases
* File-based KV stores
* Custom network databases

---

## License

MIT License
