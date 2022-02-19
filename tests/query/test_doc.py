import os.path
import re
import textwrap
import unittest

# imports necessary for eval(...) to properly find symbols in documentation
from dataclasses import dataclass, field
from typing import Optional

# star import necessary for eval(...) to properly find symbols in documentation
from pylinsql.query.core import *
from pylinsql.query.query import *

from tests.database import Address, Person, PersonCity

# global variables necessary for eval(...) to properly find entities for examples
a = Address(id=1, city="Budapest")
p = Person(
    id=1,
    family_name="Alpha",
    given_name="Omega",
    birth_date=datetime.date(1989, 10, 23),
    perm_address_id=1,
    temp_address_id=1,
)
pc = PersonCity(family_name="Beta", given_name="Boolean", city="Budapest")


def collapse_whitespace(text: str) -> str:
    "Collapse leading/trailing whitespace and newlines into a single space character."

    return " ".join(filter(None, (line.strip() for line in text.split("\n"))))


class TestDocumentation(unittest.TestCase):
    def assertQueryIs(self, query_expr: Query, sql_string: str):
        self.assertEqual(query_expr.sql, sql_string)

    def test_doc(self):
        # captures code blocks with optional leading indentation
        py_blocks = re.compile(
            r"""
                ^(\s*)```python$\n
                (?P<python>
                    (?:^\1(?!```).*$\n)*
                )
                ^\1```$\n
            """,
            re.MULTILINE | re.VERBOSE,
        )

        # captures pairs of corresponding Python and SQL code blocks
        py_sql_blocks = re.compile(
            r"""
                ^```python$\n
                (?P<python>
                    (?:^(?!```).*$\n)*
                )
                ^```$\n
                (?:^(?!```).*$\n)*
                ^```sql$\n
                (?P<sql>
                    (?:^(?!```).*$\n)*
                )
                ^```$\n
            """,
            re.MULTILINE | re.VERBOSE,
        )

        with open(os.path.join("README.md"), "r") as f:
            text = f.read()

        # verify Python code blocks
        count = 0
        for m in py_blocks.finditer(text):
            matches = m.groupdict()
            code = matches["python"]
            try:
                exec(code)
            except SyntaxError:
                # enclose async statements in async function block
                code = "async def test():\n" + textwrap.indent(code, "    ")
                exec(code)

            count += 1

        self.assertGreater(count, 0)
        self.assertEqual(count, text.count("```python"))

        # verify pairs of Python and SQL code blocks
        count = 0
        for m in py_sql_blocks.finditer(text):
            matches = m.groupdict()

            expr = eval(matches["python"])
            sql = collapse_whitespace(matches["sql"])
            self.assertQueryIs(expr, sql)

            count += 1

        self.assertGreater(count, 0)
        self.assertEqual(count, text.count("```sql"))


if __name__ == "__main__":
    unittest.main()
