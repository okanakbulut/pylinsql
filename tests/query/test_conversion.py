import unittest
from decimal import Decimal

from pylinsql.generator.conversion import sql_to_python_type
from strong_typing.auxiliary import (
    MaxLength,
    Precision,
    int16,
    int32,
    int64,
    python_type_to_str,
)
from typing_extensions import Annotated


class TestConversion(unittest.TestCase):
    def test_sql_to_python(self):
        self.assertIs(sql_to_python_type("boolean"), bool)
        self.assertIs(sql_to_python_type("smallint"), int16)
        self.assertIs(sql_to_python_type("integer"), int32)
        self.assertIs(sql_to_python_type("bigint"), int64)
        self.assertEqual(
            sql_to_python_type("decimal(64)"), Annotated[Decimal, Precision(64)]
        )
        self.assertEqual(
            sql_to_python_type("character varying(64)"), Annotated[str, MaxLength(64)]
        )

    def test_python_to_str(self):
        self.assertEqual(python_type_to_str(bool), "bool")
        self.assertEqual(python_type_to_str(int16), "int16")
        self.assertEqual(python_type_to_str(int32), "int32")
        self.assertEqual(python_type_to_str(int64), "int64")
        self.assertEqual(
            python_type_to_str(Annotated[str, MaxLength(64)]),
            "Annotated[str, MaxLength(64)]",
        )


if __name__ == "__main__":
    unittest.main()
