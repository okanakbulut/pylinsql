import os.path
import re
import unittest

# star import necessary for eval(...) to properly find symbols
from pylinsql.core import *
from pylinsql.query import *

# database import necessary for eval(...) to properly find entities for examples
from .database import Address, Person


def collapse_whitespace(text: str) -> str:
    "Collapse leading/trailing whitespace and newlines into a single space character."

    return " ".join(filter(None, (line.strip() for line in text.split("\n"))))


class TestDocumentation(unittest.TestCase):
    def assertQueryIs(self, query_expr: Query, sql_string: str):
        self.assertEqual(query_expr.sql, sql_string)

    def test_doc(self):
        regexp = re.compile(
            r"""
            ^```python$
            (?P<python>.*?)
            ^```$
            .*?
            ^```sql$
            (?P<sql>.*?)
            ^```$
        """,
            re.DOTALL | re.MULTILINE | re.VERBOSE,
        )

        with open(os.path.join("README.md"), "r") as f:
            count = 0
            text = f.read()
            for m in regexp.finditer(text):
                matches = m.groupdict()

                expr = eval(matches["python"])
                sql = collapse_whitespace(matches["sql"])
                self.assertQueryIs(expr, sql)

                count += 1

            self.assertGreater(count, 0)


if __name__ == "__main__":
    unittest.main()
