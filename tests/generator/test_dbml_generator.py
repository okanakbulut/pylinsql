import importlib
import unittest

from pylinsql.generator.dbml_generator import module_to_sql_stream


class TestDBMLGenerator(unittest.TestCase):
    def test_generator(self):
        module = importlib.import_module("test_example")

        with open("test_example.dbml", "w") as f:
            module_to_sql_stream(module, f)


if __name__ == "__main__":
    unittest.main()
