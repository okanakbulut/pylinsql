import datetime
import os.path
import unittest
from dataclasses import dataclass

import pylinsql.async_database as async_database
from pylinsql.async_database import DataAccess
from pylinsql.core import DEFAULT, entity

from .database import Person


@dataclass
class Record:
    id: int
    name: str
    view_position: str


class DatabaseTestCase(unittest.IsolatedAsyncioTestCase):
    def assertEmpty(self, obj):
        self.assertFalse(obj)

    def assertNotEmpty(self, obj):
        self.assertTrue(obj)


class TestDatabaseConnection(DatabaseTestCase):
    async def asyncTearDown(self):
        pool = await async_database.shared_pool()
        await pool.release()

    async def test_simple_query(self):
        async with async_database.connection() as conn:
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
        async with async_database.connection() as conn:
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

    async def test_pool(self):
        async with async_database.pool() as pool:
            for _ in range(0, 25):
                async with pool.connection() as connection:
                    items = await connection.raw_fetch("SELECT 42 AS value")
                    self.assertEqual(len(items), 1)
                    for item in items:
                        self.assertEqual(item["value"], 42)

    async def test_shared_pool(self):
        pool = await async_database.shared_pool()
        for _ in range(0, 25):
            async with pool.connection() as connection:
                items = await connection.raw_fetch("SELECT 42 AS value")
                self.assertEqual(len(items), 1)
                for item in items:
                    self.assertEqual(item["value"], 42)

    async def test_data_access(self):
        access = DataAccess()
        async with access.get_connection() as connection:
            items = await connection.raw_fetch("SELECT 42 AS value")
            self.assertEqual(len(items), 1)
            for item in items:
                self.assertEqual(item["value"], 42)


class TestDataTransfer(DatabaseTestCase):
    async def asyncSetUp(self):
        with open(os.path.join(os.path.dirname(__file__), "database.sql"), "r") as f:
            sql = f.read()
        async with async_database.connection() as conn:
            await conn.raw_execute(sql)

    async def asyncTearDown(self):
        pass

    async def test_select(self):
        async with async_database.connection() as conn:
            results = await conn.select(p for p in entity(Person))
            self.assertNotEmpty(results)

            result = await conn.select_first(p for p in entity(Person))
            self.assertIsNotNone(result)

            p = await conn.insert_or_select(
                Person(
                    id=DEFAULT,
                    birth_date=datetime.datetime.now(),
                    family_name="Alpha",
                    given_name="Omega",
                    perm_address_id=None,
                    temp_address_id=None,
                ),
                (
                    (p.id, p.family_name, p.given_name)
                    for p in entity(Person)
                    if p.family_name == "Alpha" and p.given_name == "Omega"
                ),
            )
            self.assertIsNotNone(p)
            self.assertGreaterEqual(p[0], 1)
            self.assertEqual(p[1], "Alpha")
            self.assertEqual(p[2], "Omega")


if __name__ == "__main__":
    unittest.main()
