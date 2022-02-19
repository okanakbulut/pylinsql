import importlib
import unittest

from pylinsql.generator.sql_generator import module_to_sql_stream


class TestSQLGenerator(unittest.TestCase):
    def test_generator(self):
        module = importlib.import_module("test_example")

        with open("test_example.sql", "w") as f:
            module_to_sql_stream(module, f)


if __name__ == "__main__":
    unittest.main()
