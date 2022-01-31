import importlib
import unittest

from pylinsql.async_database import connection
from pylinsql.code_generator import (
    catalog_to_dataclasses,
    dataclasses_to_code,
    get_catalog_schema,
)
from pylinsql.inspection import entity_classes

from tests.database_test_case import DatabaseTestCase


class TestCodeGenerator(DatabaseTestCase):
    def assertEmpty(self, obj):
        self.assertFalse(obj)

    def assertNotEmpty(self, obj):
        self.assertTrue(obj)

    async def test_generator(self):
        async with connection(self.params) as conn:
            catalog = await get_catalog_schema(conn, "public")

        self.assertNotEmpty(catalog.tables)
        self.assertIn("Address", catalog.tables)
        self.assertIn("Person", catalog.tables)
        types = catalog_to_dataclasses(catalog)
        self.assertNotEmpty(types)
        code = dataclasses_to_code(types)
        self.assertNotEmpty(code)

        with open("test_example.py", "w") as f:
            f.write(code)

        # import newly generated module file
        module = importlib.import_module("test_example")
        entity_names = entity_classes(module).keys()
        type_names = [t.__name__ for t in types]

        # check if all entity class types occur in the generated file
        for type_name in type_names:
            self.assertIn(type_name, entity_names)

        # check if all class definitions in the generated file correspond to entities
        for entity_name in entity_names:
            self.assertIn(entity_name, type_names)


if __name__ == "__main__":
    unittest.main()
