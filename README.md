# Language-Integrated SQL Queries in Python

*pylinsql* helps you write SQL queries in Python that integrate with the type checker and produce standard SQL query strings as an end result. The main idea is to take a Python generator expression such as
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

The inspiration for *pylinsql* has been to employ efficient asynchronous communication with the database engine (such as in *asyncpg*) while providing a type-safe means to formulate SELECT and INSERT queries (as in *PonyORM*).

This work is no substitute for an all-in-one boxed solution that handles database connections, performs pooling, caching, manages entity relationships, etc. (such as *SQLAlchemy*). Its purpose is to help write a SQL query in the style of C# language-integrated queries that you can then execute with a(n asynchronous) SQL engine client (e.g. *asyncpg* in Python).


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
    or year(p.birth_date) >= 1982
)
```
```sql
SELECT *
FROM "Person" AS p
WHERE p.given_name = 'John' AND p.family_name <> 'Doe' OR EXTRACT(YEAR FROM p.birth_date) >= 1982
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
    (a.city, min(p.birth_date))
    for p, a in entity(Person, Address)
    if inner_join(p.perm_address_id, a.id) and min(p.birth_date) >= date(1989, 10, 23)
)
```
```sql
SELECT a.city, MIN(p.birth_date)
FROM "Person" AS p INNER JOIN "Address" AS a ON p.perm_address_id = a.id
GROUP BY a.city
HAVING MIN(p.birth_date) >= MAKE_DATE(1989, 10, 23)
```

### Join expressions

*pylinsql* supports (inner) join, left join, right join and full (outer) join via the Python functions `inner_join`, `left_join`, `right_join` and `full_join`. These go into condition part of the generator expression. They take two parameters, both of which must be table attribute references, e.g. `p.perm_address_id` or `a1.id`:
```python
select(
    p
    for p, a1, a2 in entity(Person, Address, Address)
    if inner_join(p.perm_address_id, a1.id)
    and left_join(p.temp_address_id, a2.id)
    and ((a1.city != "London") or (a2.city != "Zürich"))
)
```

If two tables are listed as entities but not referenced by a join in the condition part, they are assumed to expand to a cross product, as in SQL.

### Select expressions

In addition to a scalar expression (single column per row) and a tuple expression (for multiple columns per row), *pylinsql* offers a convenience syntax with a `@dataclass` annotated type acting as the output.

Assume that you have a custom `@dataclass` type called `PersonCity`:
```python
@dataclass
class PersonCity:
    family_name: str
    given_name: str
    city: str
```

When executed against a database engine, the following query will produce a list of `PersonCity` instances:

```python
select(
    PersonCity(p.family_name, p.given_name, a.city)
    for p, a in entity(Person, Address)
    if inner_join(p.perm_address_id, a.id)
)
```

Positional and keyword arguments in the `@dataclass` initializer are both supported.

### Sort order

If you specify the sort order of a column with special Python functions `asc(column)` and `desc(column)`, *pylinsql* will append the appropriate `ORDER BY` clause at the end of the SQL query:

```python
select(
    (asc(p.family_name), desc(p.given_name), p.birth_date)
    for p in entity(Person)
)
```
```sql
SELECT p.family_name, p.given_name, p.birth_date
FROM "Person" AS p
ORDER BY p.family_name ASC, p.given_name DESC
```

### Aggregation functions

Several aggregation functions are available, including `avg`, `count`, `max`, `min`, `sum`, `avg_if`, `count_if`, `max_if`, `min_if` and `sum_if`.

The following example illustrates how to use simple aggregation functions:
```python
select(
    (count(p.birth_date), min(p.birth_date), max(p.birth_date))
    for p in entity(Person)
)
```

Conditional aggregation functions take a Boolean filter expression as a second parameter:
```python
select(
    (
        count_if(p.birth_date, p.given_name != "John")
    )
    for p in entity(Person)
)
```
```sql
SELECT COUNT(p.birth_date) FILTER (WHERE p.given_name <> 'John')
FROM "Person" AS p
```

### Date and time

A date constructed with `datetime.date(y, m, d)` in Python is translated to `MAKE_DATE(y, m, d)` in PostgreSQL. Likewise, `datetime.time(h, m, s)` is translated to `MAKE_TIME(h, m, s)`. Parts of a date or time can be extracted with functions like `year(dt)` or `hour(dt)`, which map to the appropriate `EXTRACT` clause in SQL. Date and time differences are also supported.

```python
select(p for p in entity(Person) if year(now() - p.birth_date) >= 18)
```
```sql
SELECT * FROM "Person" AS p WHERE EXTRACT(YEAR FROM (CURRENT_TIMESTAMP - p.birth_date)) >= 18
```

### String matching

String matching with the SQL-standard LIKE operator and PostgreSQL's regular expression match operators `~` (case sensitive) and `~*` (case insensitive match) are both supported, use Python functions `like`, `ilike`, `match` and `imatch`. The following example matches all people records whose family name ends in `can` (with a case sensitive match):

```python
select(p for p in entity(Person) if matches(p.family_name, r"can$"))
```
```sql
SELECT * FROM "Person" AS p WHERE p.family_name ~ 'can$'
```

### Executing a query

The package `async_database` contains functions to create a database connection, acquire a connection from a connection pool, and run queries in a transaction. Member functions of the class `DatabaseConnection` accept Python generator expressions the same way that `select` does in the examples above.

```python
async with async_database.connection() as conn:
    results = await conn.select(p for p in entity(Person))

    result = await conn.select_first(p for p in entity(Person))
```


## Code generator for data classes

*pylinsql* depends on Python data classes for its language-integrated query mechanism. For example, in order to execute
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
one has to define data classes corresponding to entities `Address` and `Person`:
```python
@dataclass
class Address:
    id: int
    city: str

@dataclass
class Person:
    id: int
    family_name: str
    given_name: str
    birth_date: datetime
    perm_address_id: int = field(default=...)
    temp_address_id: Optional[int] = field(default=...)
```

Defining these classes manually would be tedious work. Fortunately, *pylinsql* comes with a code generator utility that scans table schema definitions in a database, and writes corresponding Python code:
```shell
$ python3 -m pylinsql.code_generator example.py --schema public
```

The generated code takes into account type mappings, nullable types, table references and even table and column comments.

Use the switch `--help` to learn more:
```shell
$ python3 -m pylinsql.code_generator --help
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

As an example, let's consider the following Python generator expression:
```python
((p.family_name, p.given_name) for p in entity(Person) if p.given_name == "John" and p.family_name != "Doe")
```
This has a _conditional part_:
```python
p.given_name == "John" and p.family_name != "Doe"
```
and a _yield part_:
```python
(p.family_name, p.given_name)
```

*pylinsql* performs several steps to construct a SQL query string from a Python generator expression:

1. Disassembly.

    *pylinsql* uses the Python module [dis](https://docs.python.org/3/library/dis.html) to retrieve a Python generator expression as a series of instructions, which are low-level intermediate language statements such as BINARY_ADD (to add two numbers on the top of the stack), CALL_FUNCTION (to call a function with arguments on the stack), LOAD_GLOBAL (push a global variable to the top of the stack), or POP_JUMP_IF_TRUE (jump to a label if the value on the top of the stack is true).

2. Extract basic blocks.

    A basic block is a series of instructions that starts with a label (that jump instructions point to) and/or ends with a (conditional or unconditional) jump statement (e.g. POP_JUMP_IF_TRUE). For example, the following snippet shows the disassembly of our sample Python generator expression (including both the conditional and the yield part), with horizontal bars separating basic blocks. Target labels for jump instructions are shown with `>>`. The number in the first column is the instruction address.

    ```
             0 LOAD_FAST                0 (.0)
    ------------------------------------------------------------
    >>       2 FOR_ITER                38 (to 42)
             4 STORE_FAST               1 (p)
             6 LOAD_FAST                1 (p)
             8 LOAD_ATTR                0 (given_name)
            10 LOAD_CONST               0 ('John')
            12 COMPARE_OP               2 (==)
            14 POP_JUMP_IF_FALSE        2
    ------------------------------------------------------------
            16 LOAD_FAST                1 (p)
            18 LOAD_ATTR                1 (family_name)
            20 LOAD_CONST               1 ('Doe')
            22 COMPARE_OP               3 (!=)
            24 POP_JUMP_IF_FALSE        2
    ------------------------------------------------------------
            26 LOAD_FAST                1 (p)
            28 LOAD_ATTR                1 (family_name)
            30 LOAD_FAST                1 (p)
            32 LOAD_ATTR                0 (given_name)
            34 BUILD_TUPLE              2
            36 YIELD_VALUE
            38 POP_TOP
            40 JUMP_ABSOLUTE            2
    ------------------------------------------------------------
    >>      42 LOAD_CONST               2 (None)
            44 RETURN_VALUE
    ```

3. Create control flow graph (CFG).

    The control flow graph has basic blocks as nodes, and jump instruction targets as edges. For example, a basic block that ends with POP_JUMP_IF_TRUE has two outgoing edges: one points to the basic block targeted when the condition is true, and the other points to the next basic block (i.e. the next statement in the program).

    *pylinsql* uses a jump resolver to translate numeric instruction addresses into control flow graph edges.

4. Merge nodes that correspond to conditional expressions and loop conditions.

    When you have a conditional expression such as
    ```python
    p.given_name == "John" and p.family_name != "Doe"
    ```
    then the expression is represented in low-level instructions as a series of basic blocks, interconnected with jump instructions. *pylinsql* merges nodes corresponding to these basic blocks into a single compound node. For example, the above expression would become `NodeConjunction(a, b)` where `a` stands for the node representing the equality test and `b` stands for that of the inequality test, and `NodeConjunction` captures the intent of the Python keyword `and`.

    *pylinsql* merges nodes in a well-defined order. First, it merges all conditions that act like expressions (e.g. they are part of a function call). Second, it merges the condition that constitutes the loop condition of the Python generator expression. The end result is a chain of nodes, where each node is a simple node (a single basic block), or a composite node: a *sequence*, a *conjunction* or a *disjunction*. All composite nodes are also chains inside, with no branches.

5. Create an abstract syntax tree (AST).

    *pylinsql* converts the node chains into an abstract representation with the help of an evaluator. The evaluator builds a symbolic expression (e.g. `0 * 1 + 2` or `a and b and c`) from a chain of nodes and the low-level instructions stored in them.

    The evaluator maintains a stack, mimicking how the Python interpreter works. It goes through the instructions of a basic block, and manipulates the stack following the instructions. Whenever global or local symbols are referenced (e.g. constants or variable names), the evaluator pushes their symbolic representation. Any further operations are performed with this symbolic representation. For example, when encountering the instruction BINARY_ADD, which adds two numbers popping off items from the top of the stack and pushing the result, the evaluator will pop off two symbolic expressions (e.g. `Variable(a)` and `Constant(2)`), and push a new symbolic expression (e.g. `Addition(Variable(a), Constant(2))`).

    Conjunctions and disjunctions are handled in a special way. These are represented by multiple basic blocks, interconnected by conditional jump instructions.

    Whenever the AST builder encounters a conjunction, it tells the evaluator to process jump instructions as if the condition evaluated to true. In a structured expression such as `a and b and c`, this would force evaluating the rest of the expression, and not short-circuit at `a` or `a and b`. Likewise, jump instructions in blocks of a disjunction are processed as if conditions evaluated to false. In either case, the top of the stack is going to contain a symbolic expression for all the sub-expressions combined into a conjunction or disjunction, respectively, as the evaluator jumps through all basic blocks that comprise them.

6. Analyze the abstract syntax tree.

    *pylinsql* checks if the expression is well-formed, e.g. whether you join objects along existing properties (e.g. `Person` has `given_name`).

7. Emit an SQL statement.

    *pylinsql* maps Python function calls into SQL statement equivalents, e.g. `asc()` becomes `ORDER BY`, `inner_join()` maps to an `INNER JOIN` in a `FROM` clause, a condition on a `min()` becomes part of `HAVING`, `GROUP BY` is generated based on the result expressions in the original Python generator expression, etc.
    