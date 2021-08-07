import unittest

from pylinsql.core import (
    Query,
    asc,
    count,
    count_if,
    day,
    desc,
    entity,
    inner_join,
    left_join,
    max_if,
    min_if,
    month,
    now,
    p_1,
    year,
)
from pylinsql.query import insert_or_select, select

from .database import Address, Person


class TestLanguageIntegratedSQL(unittest.TestCase):
    def assertQueryIs(self, query_expr: Query, sql_string: str):
        self.assertEqual(query_expr.sql, sql_string)

    def test_example(self):
        self.assertQueryIs(
            select(
                asc(p.given_name)
                for p, a in entity(Person, Address)
                if inner_join(p.address_id, a.id)
                and (
                    (p.given_name == "John" and p.family_name != "Doe")
                    or (a.city != "London")
                )
            ),
            "SELECT p.given_name FROM Person AS p INNER JOIN Address AS a ON p.address_id = a.id WHERE p.given_name = 'John' AND p.family_name <> 'Doe' OR a.city <> 'London' ORDER BY p.given_name ASC",
        )

    def test_select(self):
        self.assertQueryIs(
            select((p.family_name, p.given_name) for p in entity(Person)),
            "SELECT p.family_name, p.given_name FROM Person AS p",
        )

    def test_select_all(self):
        self.assertQueryIs(
            select(p for p in entity(Person)),
            "SELECT * FROM Person AS p",
        )

    def test_join(self):
        self.assertQueryIs(
            select(
                p
                for p, a1, a2 in entity(Person, Address, Address)
                if inner_join(p.perm_address_id, a1.id)
                and left_join(p.temp_address_id, a2.id)
            ),
            "SELECT * FROM Person AS p INNER JOIN Address AS a1 ON p.perm_address_id = a1.id LEFT JOIN Address AS a2 ON p.temp_address_id = a2.id",
        )

    def test_join_where(self):
        self.assertQueryIs(
            select(
                p
                for p, a1, a2 in entity(Person, Address, Address)
                if inner_join(p.perm_address_id, a1.id)
                and left_join(p.temp_address_id, a2.id)
                and ((a1.city != "London") or (a2.city != "Zürich"))
            ),
            "SELECT * FROM Person AS p INNER JOIN Address AS a1 ON p.perm_address_id = a1.id LEFT JOIN Address AS a2 ON p.temp_address_id = a2.id WHERE a1.city <> 'London' OR a2.city <> 'Zürich'",
        )

    def test_subquery(self):
        subquery_expr = select(a.id for a in entity(Address) if a.city == "London")
        query_expr = select(
            p for p in entity(Person) if p.perm_address_id in subquery_expr
        )
        self.assertQueryIs(
            query_expr,
            "SELECT * FROM Person AS p WHERE p.perm_address_id IN (SELECT a.id FROM Address AS a WHERE a.city = 'London')",
        )

    def test_where_conj_disj(self):
        self.assertQueryIs(
            select(
                p
                for p in entity(Person)
                if p.given_name == "John"
                and p.family_name != "Doe"
                or (2021 - p.birth_year >= 18)
            ),
            "SELECT * FROM Person AS p WHERE p.given_name = 'John' AND p.family_name <> 'Doe' OR 2021 - p.birth_year >= 18",
        )

    def test_where_having(self):
        self.assertQueryIs(
            select(
                p
                for p in entity(Person)
                if p.given_name == "John" and min(p.birth_year) >= 1980
            ),
            "SELECT * FROM Person AS p WHERE p.given_name = 'John' HAVING MIN(p.birth_year) >= 1980",
        )

    def test_group_by(self):
        self.assertQueryIs(
            select(
                (a.city, min(p.birth_year))
                for p, a in entity(Person, Address)
                if inner_join(p.perm_address_id, a.id) and min(p.birth_year) >= 1980
            ),
            "SELECT a.city, MIN(p.birth_year) FROM Person AS p INNER JOIN Address AS a ON p.perm_address_id = a.id GROUP BY a.city HAVING MIN(p.birth_year) >= 1980",
        )

    def test_order_by(self):
        self.assertQueryIs(
            select(
                (asc(p.family_name), desc(p.given_name), p.birth_date)
                for p in entity(Person)
            ),
            "SELECT p.family_name, p.given_name, p.birth_date FROM Person AS p ORDER BY p.family_name ASC, p.given_name DESC",
        )

    def test_aggregate(self):
        self.assertQueryIs(
            select(
                (count(p.birth_date), min(p.birth_date), max(p.birth_date))
                for p in entity(Person)
            ),
            "SELECT COUNT(p.birth_date), MIN(p.birth_date), MAX(p.birth_date) FROM Person AS p",
        )

    def test_conditional_aggregate(self):
        self.assertQueryIs(
            select(
                (
                    count_if(p.birth_date, p.given_name != "John"),
                    min_if(p.birth_date, p.given_name != "John"),
                    max_if(p.birth_date, p.given_name != "John"),
                )
                for p in entity(Person)
            ),
            "SELECT COUNT(p.birth_date) FILTER (WHERE p.given_name <> 'John'), MIN(p.birth_date) FILTER (WHERE p.given_name <> 'John'), MAX(p.birth_date) FILTER (WHERE p.given_name <> 'John') FROM Person AS p",
        )

    def test_parameterized(self):
        self.assertQueryIs(
            select((p for p in entity(Person) if p.given_name == p_1)),
            "SELECT * FROM Person AS p WHERE p.given_name = $1",
        )

    def test_where_date(self):
        self.assertQueryIs(
            select(
                p
                for p in entity(Person)
                if year(p.birth_date) >= 1980
                and month(p.birth_date) > 6
                and day(p.birth_date) <= 15
            ),
            "SELECT * FROM Person AS p WHERE EXTRACT(YEAR FROM p.birth_date) >= 1980 AND EXTRACT(MONTH FROM p.birth_date) > 6 AND EXTRACT(DAY FROM p.birth_date) <= 15",
        )

    def test_where_date_time_delta(self):
        self.assertQueryIs(
            select(p for p in entity(Person) if year(now() - p.birth_date) >= 18),
            "SELECT * FROM Person AS p WHERE EXTRACT(YEAR FROM (CURRENT_TIMESTAMP - p.birth_date)) >= 18",
        )

    def test_insert_or_select(self):
        self.assertQueryIs(
            insert_or_select(
                Address(id=1, city="Budapest"),
                (a for a in entity(Address) if a.city == "Budapest"),
            ),
            "WITH select_query AS (SELECT * FROM Address AS a WHERE a.city = 'Budapest'), insert_query AS (INSERT INTO Address AS a (id, city) SELECT $1, $2 WHERE NOT EXISTS (SELECT * FROM select_query) RETURNING *) SELECT * FROM select_query UNION ALL SELECT * FROM insert_query",
        )


if __name__ == "__main__":
    unittest.main()
