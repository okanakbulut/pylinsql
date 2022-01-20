import unittest

from pylinsql.async_database import connection
from pylinsql.code_generator import (
    catalog_to_dataclasses,
    dataclasses_to_code,
    get_catalog_schema,
)
from pylinsql.timing import timing

from tests.database_test_case import DatabaseTestCase


class TestCodeGenerator(DatabaseTestCase):
    def assertEmpty(self, obj):
        self.assertFalse(obj)

    def assertNotEmpty(self, obj):
        self.assertTrue(obj)

    @timing
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


if __name__ == "__main__":
    unittest.main()
