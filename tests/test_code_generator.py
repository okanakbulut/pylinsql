import unittest

from pylinsql.async_database import ConnectionParameters, connection
from pylinsql.code_generator import (
    catalog_to_dataclasses,
    dataclasses_to_code,
    get_catalog_schema,
)


class TestCodeGenerator(unittest.IsolatedAsyncioTestCase):
    def assertEmpty(self, obj):
        self.assertFalse(obj)

    def assertNotEmpty(self, obj):
        self.assertTrue(obj)

    async def test_generator(self):
        async with connection(ConnectionParameters()) as conn:
            catalog = await get_catalog_schema(conn, "public")
            self.assertNotEmpty(catalog.tables)
            self.assertIn("Address", catalog.tables)
            self.assertIn("Person", catalog.tables)
            types = catalog_to_dataclasses(catalog)
            self.assertNotEmpty(types)
            code = dataclasses_to_code(types)
            self.assertNotEmpty(code)


if __name__ == "__main__":
    unittest.main()
