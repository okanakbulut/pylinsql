import unittest
from datetime import date
from typing import Generator

from pylinsql.core import (
    Query,
    QueryTypeError,
    asc,
    avg,
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
from pylinsql.query import cache_info, insert_or_select, select

from tests.database import Address, Person, PersonCity, PersonCountryCity


class TestLanguageIntegratedSQL(unittest.TestCase):
    def assertQueryIs(self, query_expr: Query, sql_string: str):
        self.assertEqual(query_expr.sql, sql_string)

    def get_example_expr(self) -> Generator[str, None, None]:
        expr = (
            asc(p.given_name)
            for p, a in entity(Person, Address)
            if inner_join(p.address_id, a.id)
            and (
                (p.given_name == "John" and p.family_name != "Doe")
                or (a.city != "London")
            )
        )
        return expr

    def test_example(self):
        self.assertQueryIs(
            select(self.get_example_expr()),
            """SELECT p.given_name FROM "Person" AS p INNER JOIN "Address" AS a ON p.address_id = a.id WHERE p.given_name = 'John' AND p.family_name <> 'Doe' OR a.city <> 'London' ORDER BY p.given_name ASC""",
        )

    def test_select(self):
        self.assertQueryIs(
            select((p.family_name, p.given_name) for p in entity(Person)),
            """SELECT p.family_name, p.given_name FROM "Person" AS p""",
        )

    def test_select_all(self):
        self.assertQueryIs(
            select(p for p in entity(Person)), """SELECT * FROM "Person" AS p"""
        )

    def test_join(self):
        self.assertQueryIs(
            select(
                p
                for p, a1, a2 in entity(Person, Address, Address)
                if inner_join(p.perm_address_id, a1.id)
                and left_join(p.temp_address_id, a2.id)
            ),
            """SELECT * FROM "Person" AS p INNER JOIN "Address" AS a1 ON p.perm_address_id = a1.id LEFT JOIN "Address" AS a2 ON p.temp_address_id = a2.id""",
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
            """SELECT * FROM "Person" AS p INNER JOIN "Address" AS a1 ON p.perm_address_id = a1.id LEFT JOIN "Address" AS a2 ON p.temp_address_id = a2.id WHERE a1.city <> 'London' OR a2.city <> 'Zürich'""",
        )

    def test_select_dataclass(self):
        query = select(
            PersonCity(p.family_name, p.given_name, a.city)
            for p, a in entity(Person, Address)
            if inner_join(p.perm_address_id, a.id)
        )
        self.assertQueryIs(
            query,
            """SELECT p.family_name, p.given_name, a.city FROM "Person" AS p INNER JOIN "Address" AS a ON p.perm_address_id = a.id""",
        )
        self.assertEqual(query.typ, PersonCity)

    def test_select_dataclass_keyword(self):
        query = select(
            PersonCity(given_name=p.given_name, family_name=p.family_name, city=a.city)
            for p, a in entity(Person, Address)
            if inner_join(p.perm_address_id, a.id)
        )
        self.assertQueryIs(
            query,
            """SELECT p.family_name, p.given_name, a.city FROM "Person" AS p INNER JOIN "Address" AS a ON p.perm_address_id = a.id""",
        )
        self.assertEqual(query.typ, PersonCity)

        query = select(
            PersonCountryCity(
                given_name=p.given_name, family_name=p.family_name, city=a.city
            )
            for p, a in entity(Person, Address)
            if inner_join(p.perm_address_id, a.id)
        )
        self.assertQueryIs(
            query,
            """SELECT p.family_name, p.given_name, NULL, a.city FROM "Person" AS p INNER JOIN "Address" AS a ON p.perm_address_id = a.id""",
        )
        self.assertEqual(query.typ, PersonCountryCity)

    def test_subquery(self):
        subquery_expr = select(a.id for a in entity(Address) if a.city == "London")
        query_expr = select(
            p for p in entity(Person) if p.perm_address_id in subquery_expr
        )
        self.assertQueryIs(
            query_expr,
            """SELECT * FROM "Person" AS p WHERE p.perm_address_id IN (SELECT a.id FROM "Address" AS a WHERE a.city = 'London')""",
        )

    def test_where_conj_disj(self):
        self.assertQueryIs(
            select(
                p
                for p in entity(Person)
                if p.given_name == "John"
                and p.family_name != "Doe"
                or (p.perm_address_id is not None)
            ),
            """SELECT * FROM "Person" AS p WHERE p.given_name = 'John' AND p.family_name <> 'Doe' OR p.perm_address_id IS NOT NULL""",
        )

    def test_where_comparison_chain(self):
        self.assertQueryIs(
            select(
                p
                for p in entity(Person)
                if date(1980, 1, 1) <= p.birth_date <= date(1990, 1, 1)
            ),
            """SELECT * FROM "Person" AS p WHERE MAKE_DATE(1980, 1, 1) <= p.birth_date AND p.birth_date <= MAKE_DATE(1990, 1, 1)""",
        )

    def test_where_mixed_comparison_chain(self):
        self.assertQueryIs(
            select((p for p in entity(Person) if True == True != False)),
            """SELECT * FROM "Person" AS p WHERE True = True AND True <> False""",
        )

    def test_where_having(self):
        self.assertQueryIs(
            select(
                min(p.birth_date)
                for p in entity(Person)
                if p.given_name == "John" and min(p.birth_date) >= date(1989, 10, 23)
            ),
            """SELECT MIN(p.birth_date) FROM "Person" AS p WHERE p.given_name = 'John' HAVING MIN(p.birth_date) >= MAKE_DATE(1989, 10, 23)""",
        )

    def test_group_by(self):
        self.assertQueryIs(
            select(
                (a.city, min(p.birth_date))
                for p, a in entity(Person, Address)
                if inner_join(p.perm_address_id, a.id)
                and min(p.birth_date) >= date(1989, 10, 23)
            ),
            """SELECT a.city, MIN(p.birth_date) FROM "Person" AS p INNER JOIN "Address" AS a ON p.perm_address_id = a.id GROUP BY a.city HAVING MIN(p.birth_date) >= MAKE_DATE(1989, 10, 23)""",
        )

    def test_order_by(self):
        self.assertQueryIs(
            select(
                (asc(p.family_name), desc(p.given_name), p.birth_date)
                for p in entity(Person)
            ),
            """SELECT p.family_name, p.given_name, p.birth_date FROM "Person" AS p ORDER BY p.family_name ASC, p.given_name DESC""",
        )

    def test_aggregate(self):
        self.assertQueryIs(
            select(
                (count(p.birth_date), min(p.birth_date), max(p.birth_date))
                for p in entity(Person)
            ),
            """SELECT COUNT(p.birth_date), MIN(p.birth_date), MAX(p.birth_date) FROM "Person" AS p""",
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
            """SELECT COUNT(p.birth_date) FILTER (WHERE p.given_name <> 'John'), MIN(p.birth_date) FILTER (WHERE p.given_name <> 'John'), MAX(p.birth_date) FILTER (WHERE p.given_name <> 'John') FROM "Person" AS p""",
        )

    def test_parameterized(self):
        self.assertQueryIs(
            select((p for p in entity(Person) if p.given_name == p_1)),
            """SELECT * FROM "Person" AS p WHERE p.given_name = $1""",
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
            """SELECT * FROM "Person" AS p WHERE EXTRACT(YEAR FROM p.birth_date) >= 1980 AND EXTRACT(MONTH FROM p.birth_date) > 6 AND EXTRACT(DAY FROM p.birth_date) <= 15""",
        )

    def test_where_date_time_delta(self):
        self.assertQueryIs(
            select(p for p in entity(Person) if year(now() - p.birth_date) >= 18),
            """SELECT * FROM "Person" AS p WHERE EXTRACT(YEAR FROM (CURRENT_TIMESTAMP - p.birth_date)) >= 18""",
        )

    def test_insert_or_select(self):
        self.assertQueryIs(
            insert_or_select(
                Address(id=1, city="Budapest"),
                (a for a in entity(Address) if a.city == "Budapest"),
            ),
            """WITH select_query AS (SELECT * FROM "Address" AS a WHERE a.city = 'Budapest'), insert_query AS (INSERT INTO "Address" AS a (id, city) SELECT $1, $2 WHERE NOT EXISTS (SELECT * FROM select_query) RETURNING *) SELECT * FROM select_query UNION ALL SELECT * FROM insert_query""",
        )

    def test_expression_cache(self):
        # make sure the expression is in the cache
        query1 = select(self.get_example_expr()).sql
        cache1 = cache_info()

        # verify additional queries with the same expression cause hits, not misses
        query2 = select(self.get_example_expr()).sql
        cache2 = cache_info()
        self.assertEqual(cache1.hits + 1, cache2.hits)
        self.assertEqual(cache1.misses, cache2.misses)

        # verify query string is the same
        self.assertEqual(query1, query2)

    def disabled_test_conj_in_yield(self):
        self.assertQueryIs(
            select(count_if(p.id, False or True and True) for p in entity(Person)),
            """SELECT COUNT(p.id) FILTER (WHERE False OR True AND True) FROM "Person" AS p""",
        )

    def disabled_test_comparison_chain_in_yield(self):
        #       0 LOAD_FAST                0 (.0)
        # >>    2 FOR_ITER                56 (to 60)
        #       4 STORE_FAST               1 (p)
        #       6 LOAD_GLOBAL              0 (count_if)
        #       8 LOAD_FAST                1 (p)
        #      10 LOAD_ATTR                1 (id)
        #      12 LOAD_GLOBAL              2 (date)
        #      14 LOAD_CONST               0 (1980)
        #      16 LOAD_CONST               1 (1)
        #      18 LOAD_CONST               1 (1)
        #      20 CALL_FUNCTION            3
        #      22 LOAD_FAST                1 (p)
        #      24 LOAD_ATTR                3 (birth_date)
        #      26 DUP_TOP
        #      28 ROT_THREE
        #      30 COMPARE_OP               1 (<=)
        #      32 JUMP_IF_FALSE_OR_POP    48
        #      34 LOAD_GLOBAL              2 (date)
        #      36 LOAD_CONST               2 (1990)
        #      38 LOAD_CONST               1 (1)
        #      40 LOAD_CONST               1 (1)
        #      42 CALL_FUNCTION            3
        #      44 COMPARE_OP               1 (<=)
        #      46 JUMP_FORWARD             4 (to 52)
        # >>   48 ROT_TWO
        #      50 POP_TOP
        # >>   52 CALL_FUNCTION            2
        #      54 YIELD_VALUE
        #      56 POP_TOP
        #      58 JUMP_ABSOLUTE            2
        # >>   60 LOAD_CONST               3 (None)
        #      62 RETURN_VALUE
        self.assertQueryIs(
            select(
                count_if(p.id, date(1980, 1, 1) <= p.birth_date <= date(1990, 1, 1))
                for p in entity(Person)
            ),
            """SELECT COUNT(p.id) FILTER (WHERE MAKE_DATE(1980, 1, 1) <= p.birth_date AND p.birth_date <= MAKE_DATE(1990, 1, 1)) FROM "Person" AS p""",
        )

    def test_fail_wrong_type(self):
        with self.assertRaises(TypeError):
            select(entity(Person))

    def test_fail_mixed_context(self):
        with self.assertRaises(QueryTypeError):
            select(p for p in entity(Person) if min(p.birth_date) >= p.birth_date)

    def test_fail_nested_aggregation(self):
        with self.assertRaises(QueryTypeError):
            select(
                p
                for p in entity(Person)
                if min(max(p.birth_date)) >= date(1989, 10, 23)
            )

    def test_fail_wrong_join(self):
        with self.assertRaises(QueryTypeError):
            select(
                (p, a)
                for p, a in entity(Person, Address)
                if inner_join(p.perm_address_id, a.id)
                or inner_join(p.temp_address_id, a.id)
            )

    def test_fail_wrong_order(self):
        with self.assertRaises(QueryTypeError):
            select(
                p
                for p in entity(Person)
                if min(asc(p.birth_date)) >= date(1989, 10, 23)
            )


if __name__ == "__main__":
    unittest.main()
