# Language-Integrated SQL Queries in Python

The purpose of this package is to help write SQL queries in Python that integrate with the type checker and produce standard SQL query strings as an end result. The main idea is to take a Python generator expression such as
```python
select(
    asc(p.given_name)
    for p, a in entity(Person, Address)
    if inner_join(p.address_id, a.id)
    and (
        (p.given_name == "John" and p.family_name != "Doe")
        or (a.city != "London")
    )
)
```
and transform it into a SQL query such as
```sql
SELECT p.given_name
FROM "Person" AS p INNER JOIN "Address" AS a ON p.address_id = a.id
WHERE p.given_name = 'John' AND p.family_name <> 'Doe' OR a.city <> 'London'
ORDER BY p.given_name ASC
```

Using a language-integrated query formalism (analogous to LINQ in C#), users can write queries in a format that is transparent to lint tools, and identify errors early. The query expressions map to SQL statement strings, which allows for constant-time look-up, making *pylinsql* incur almost zero additional run-time cost over writing raw SQL statements while providing type safety.


## Objectives

The inspiration for *pylinsql* has been to employ efficient asynchronous communication with the database engine (such as in *asyncpg*) while providing a type-safe means to formulate SELECT and INSERT queries (as in PonyORM).

This work is no substitute for an all-in-one boxed solution that handles database connections, performs pooling, caching, manages entity relationships, etc. (such as SQLAlchemy). Its purpose is to help write a SQL query in the style of C# language-integrated queries that you can then execute with a(n asynchronous) SQL engine client (e.g. *asyncpg* in Python).


## Usage

Expressions preceding `for` in a Python generator expression go into `SELECT` in SQL:
```python
select((p.family_name, p.given_name) for p in entity(Person))
```
```sql
SELECT p.family_name, p.given_name
FROM "Person" AS p
```

If you have an entity variable preceding `for`, it will expand into all properties of that entity:
```python
select(p for p in entity(Person))
```
```sql
SELECT *
FROM "Person" AS p
```

Boolean expressions in the condition part of a Python generator expression (i.e. following `if`) normally go into the `WHERE` clause:
```python
select(
    p
    for p in entity(Person)
    if p.given_name == "John"
    and p.family_name != "Doe"
    or (2021 - p.birth_year >= 18)
)
```
```sql
SELECT *
FROM "Person" AS p
WHERE p.given_name = 'John' AND p.family_name <> 'Doe' OR 2021 - p.birth_year >= 18
```

The conditional part also accepts special functions `inner_join`, `left_join`, `right_join`, etc. to create join expressions in SQL. These special functions are only allowed in the condition part of the generator expression but not elsewhere. You can combine several join conditions with Python's `and`.
```python
select(
    p
    for p, a1, a2 in entity(Person, Address, Address)
    if inner_join(p.perm_address_id, a1.id)
    and left_join(p.temp_address_id, a2.id)
)
```
```sql
SELECT *
FROM "Person" AS p
    INNER JOIN "Address" AS a1 ON p.perm_address_id = a1.id
    LEFT JOIN "Address" AS a2 ON p.temp_address_id = a2.id
```

You can also use aggregation functions. Expressions that are not aggregated automatically go into the `GROUP BY` clause. If you have a condition that involves an aggregated expression, it becomes part of the `HAVING` clause.
```python
select(
    (a.city, min(p.birth_year))
    for p, a in entity(Person, Address)
    if inner_join(p.perm_address_id, a.id) and min(p.birth_year) >= 1980
)
```
```sql
SELECT a.city, MIN(p.birth_year)
FROM "Person" AS p INNER JOIN "Address" AS a ON p.perm_address_id = a.id
GROUP BY a.city
HAVING MIN(p.birth_year) >= 1980
```


## Background and related work

[psycopg2](https://pypi.org/project/psycopg2/) provides a way to piece together SQL queries using composable primitive objects like `Identifier` (e.g. a table name), `Literal` (e.g. an integer or string value), `Placeholder` (in a prepared statement) and `SQL` (represents a SQL statement segment). It also provides a mechanism to establish a synchronous connection to a PostgreSQL server.

[asyncpg](https://magicstack.github.io/asyncpg/) is a library that exposes an asynchronous connection to a PostgreSQL server utilizing Python's *asyncio* services. If queries or parameterized queries are available as a string, *asyncpg* can execute them efficiently.

[PonyORM](https://ponyorm.org/) is an object-relational mapping (ORM) library that uses a similar syntax based on Python generator expressions. It is a full-fledged ORM solution that uses a synchronous connection to a SQL server.

[SQLAlchemy](https://www.sqlalchemy.org) is the most widely-used object-relational mapping with a rich set of features (organized in a hierarchy), and an ability to use asynchronous database connections. Unfortunately, the query syntax is rather verbose and does not look like a neat Python expression.

The disassembling approach to reverse-engineer the abstract syntax tree (AST) from the control flow graph (CFG) is similar to that used in [PonyORM](https://github.com/ponyorm/pony/blob/orm/pony/orm/decompiling.py).

The consistent coloring of incoming green/red edges of nodes in the abstract node graph is discussed in detail in [Decompiling Boolean Expressions from Java Bytecode](https://www.cse.iitd.ac.in/~sak/reports/isec2016-paper.pdf), specifically *Algorithm 2*.

For further reading, check out [No More Gotos: Decompilation Using Pattern-Independent Control-Flow Structuring
and Semantics-Preserving Transformations](https://www.ndss-symposium.org/wp-content/uploads/2017/09/11_4_2.pdf). Also, [Solving the structured control flow problem once and for all](https://medium.com/leaningtech/solving-the-structured-control-flow-problem-once-and-for-all-5123117b1ee2) might be of interest.


## Implementation details

*pylinsql* utilizes some more advanced features and programming language concepts such as Python intermediate language, low-level code analysis, graph theory and parsers/generators.

*pylinsql* performs several steps to construct a SQL query string from a Python generator expression:

1. De-compilation.

    *pylinsql* uses the Python module [dis](https://docs.python.org/3/library/dis.html) to retrieve a Python generator expression as a series of instructions, which are low-level intermediate language statements such as BINARY_ADD (to add two numbers on the top of the stack), CALL_FUNCTION (to call a function with arguments on the stack), LOAD_GLOBAL (push a global variable to the top of the stack), or POP_JUMP_IF_TRUE (jump to a label if the value on the top of the stack is true).

2. Extract basic blocks to create control flow graph (CFG).

    A basic block is a series of instructions that usually starts with a label (that jump instructions point to), and ends with a (conditional or unconditional) jump statement (e.g. POP_JUMP_IF_TRUE). *pylinsql* creates basic blocks from the conditional part of a Python generator expression. For example, given the Python generator expression,
    ```python
    p for p in entity(Person) if p.given_name == "John" and p.family_name != "Doe"
    ```
    it extracts the instructions that constitute the part
    ```python
    if p.given_name == "John" and p.family_name != "Doe"
    ```

    The control flow graph has basic blocks as nodes, and jump instruction labels as edges. For example, a basic block that ends with POP_JUMP_IF_TRUE has two outgoing edges: one points to the basic block targeted when the condition is true, and the other points to the next basic block (i.e. the next statement in the program).

3. Create an abstract syntax tree (AST).

    *pylinsql* connects the basic blocks along jump instructions, and uses the jump instruction conditions to create a single abstract syntax expression. The expression no longer contains any jumps, instead it is a series of conjunctions (`and`), disjunctions (`or`) and negations (`not`), which join boolean expressions (e.g. comparisons).

4. Analyze the abstract syntax tree.

    *pylinsql* checks if the expression is well-formed, e.g. whether you join objects along existing properties (e.g. `Person` has `given_name`).

5. Emit an SQL statement.

    *pylinsql* maps Python function calls into SQL statement equivalents, e.g. `asc()` becomes `ORDER BY`, `inner_join()` maps to an `INNER JOIN` in a `FROM` clause, a condition on a `min()` becomes part of `HAVING`, `GROUP BY` is generated based on the result expressions in the original Python generator expression, etc.
    