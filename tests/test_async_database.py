import unittest
from dataclasses import dataclass

from pylinsql.async_database import DataAccess, connection


@dataclass
class Record:
    id: int
    name: str
    view_position: str


class TestDatabaseConnection(unittest.IsolatedAsyncioTestCase):
    def assertEmpty(self, obj):
        self.assertFalse(obj)

    def assertNotEmpty(self, obj):
        self.assertTrue(obj)

    async def test_simple_query(self):
        async with connection() as conn:
            query = """
                WITH sample (id, value) AS (VALUES
                    (1, 'first'),
                    (2, 'second'),
                    (3, 'third')
                ) 
                SELECT * FROM sample
            """
            values = await conn.typed_fetch(Record, query)
            self.assertNotEmpty(values)

    async def test_parameterized_query(self):
        async with connection() as conn:
            query = """
                WITH sample (id, value) AS (VALUES
                    (1, 'first'),
                    (2, 'second'),
                    (3, 'third')
                ) 
                SELECT * FROM sample WHERE sample.value = $1
            """
            values = await conn.typed_fetch(Record, query, "first")
            self.assertNotEmpty(values)
            values = await conn.typed_fetch(Record, query, "fourth")
            self.assertEmpty(values)

    async def test_data_access(self):
        access = DataAccess()
        async with access.get_connection() as connection:
            items = await connection.raw_fetch("SELECT 42 AS value")
            self.assertEqual(len(items), 1)
            for item in items:
                self.assertEqual(item["value"], 42)


if __name__ == "__main__":
    unittest.main()
