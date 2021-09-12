import datetime
import os.path
import unittest
from dataclasses import dataclass

import pylinsql.async_database as async_database
from pylinsql.async_database import DataAccess
from pylinsql.core import DEFAULT, count, entity, inner_join

from tests.database import Address, Person, PersonCity
from tests.database_test_case import DatabaseTestCase


@dataclass
class Record:
    id: int
    name: str
    view_position: str


@dataclass
class PersonFullName:
    id: int
    family_name: str
    given_name: str


class TestDatabaseConnection(DatabaseTestCase):
    async def asyncTearDown(self):
        pool = await async_database.shared_pool(self.params)
        await pool.release()

    async def test_simple_query(self):
        async with async_database.connection(self.params) as conn:
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
        async with async_database.connection(self.params) as conn:
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
        async with async_database.pool(self.params) as pool:
            for _ in range(0, 25):
                async with pool.connection() as connection:
                    items = await connection.raw_fetch("SELECT 42 AS value")
                    self.assertEqual(len(items), 1)
                    for item in items:
                        self.assertEqual(item["value"], 42)

    async def test_shared_pool(self):
        pool = await async_database.shared_pool(self.params)
        for _ in range(0, 25):
            async with pool.connection() as connection:
                items = await connection.raw_fetch("SELECT 42 AS value")
                self.assertEqual(len(items), 1)
                for item in items:
                    self.assertEqual(item["value"], 42)

    async def test_data_access(self):
        access = DataAccess(self.params)
        async with access.get_connection() as connection:
            items = await connection.raw_fetch("SELECT 42 AS value")
            self.assertEqual(len(items), 1)
            for item in items:
                self.assertEqual(item["value"], 42)


class TestDataTransfer(DatabaseTestCase):
    async def asyncSetUp(self):
        with open(os.path.join(os.path.dirname(__file__), "database.sql"), "r") as f:
            sql = f.read()
        async with async_database.connection(self.params) as conn:
            await conn.raw_execute(sql)

    async def asyncTearDown(self):
        pass

    async def test_select(self):
        async with async_database.connection(self.params) as conn:
            results = await conn.select(p for p in entity(Person))
            self.assertNotEmpty(results)

            result = await conn.select_first(p for p in entity(Person))
            self.assertIsNotNone(result)

            result = await conn.select_first(
                PersonCity(p.family_name, p.given_name, a.city)
                for p, a in entity(Person, Address)
                if inner_join(p.perm_address_id, a.id)
            )
            self.assertIsNotNone(result)
            self.assertIsInstance(result, PersonCity)
            self.assertEqual(result.family_name, "American")
            self.assertEqual(result.given_name, "Abel")
            self.assertEqual(result.city, "Aberdeen")

            result = await conn.select_first(
                PersonCity(
                    given_name=p.given_name, family_name=p.family_name, city=a.city
                )
                for p, a in entity(Person, Address)
                if inner_join(p.perm_address_id, a.id)
            )
            self.assertIsNotNone(result)
            self.assertIsInstance(result, PersonCity)
            self.assertEqual(result.family_name, "American")
            self.assertEqual(result.given_name, "Abel")
            self.assertEqual(result.city, "Aberdeen")

    async def test_insert_or_select(self):
        async with async_database.connection(self.params) as conn:
            person_count = await conn.select_first(
                count(p.id) for p in entity(Person) if p.family_name == "Alpha"
            )
            self.assertEqual(person_count, 0)

            p = await conn.insert_or_select(
                Person(
                    id=DEFAULT,
                    birth_date=datetime.datetime.now(),
                    family_name="Alpha",
                    given_name="Omega",
                    perm_address_id=1,
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

            person_count = await conn.select_first(
                count(p.id) for p in entity(Person) if p.family_name == "Alpha"
            )
            self.assertEqual(person_count, 1)

            p = await conn.insert_or_select(
                Person(
                    id=DEFAULT,
                    birth_date=datetime.datetime.now(),
                    family_name="Alpha",
                    given_name="Omega",
                    perm_address_id=1,
                    temp_address_id=None,
                ),
                (
                    PersonFullName(p.id, p.family_name, p.given_name)
                    for p in entity(Person)
                    if p.family_name == "Alpha" and p.given_name == "Omega"
                ),
            )
            self.assertIsNotNone(p)
            self.assertGreaterEqual(p.id, 1)
            self.assertEqual(p.family_name, "Alpha")
            self.assertEqual(p.given_name, "Omega")

            person_count = await conn.select_first(
                count(p.id) for p in entity(Person) if p.family_name == "Alpha"
            )
            self.assertEqual(person_count, 1)

    async def test_insert_or_ignore(self):
        async with async_database.connection(self.params) as conn:
            person_count = await conn.select_first(
                count(p.id) for p in entity(Person) if p.family_name == "Alpha"
            )
            self.assertEqual(person_count, 0)

            await conn.insert_or_ignore(
                Person(
                    id=DEFAULT,
                    birth_date=datetime.datetime.now(),
                    family_name="Alpha",
                    given_name="Omega",
                    perm_address_id=1,
                    temp_address_id=None,
                )
            )

            person_count = await conn.select_first(
                count(p.id) for p in entity(Person) if p.family_name == "Alpha"
            )
            self.assertEqual(person_count, 1)

            person = Person(
                id=100,
                birth_date=datetime.datetime.now(),
                family_name="Beta",
                given_name="Psi",
                perm_address_id=1,
                temp_address_id=None,
            )
            await conn.insert_or_ignore(person)

            person_count = await conn.select_first(
                count(p.id) for p in entity(Person) if p.family_name == "Beta"
            )
            self.assertEqual(person_count, 1)

            await conn.insert_or_ignore(person)

            person_count = await conn.select_first(
                count(p.id) for p in entity(Person) if p.family_name == "Beta"
            )
            self.assertEqual(person_count, 1)


if __name__ == "__main__":
    unittest.main()
