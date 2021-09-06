from pylinsql.async_database import ConnectionParameters
import unittest


class DatabaseTestCase(unittest.IsolatedAsyncioTestCase):
    params: ConnectionParameters

    def __init__(self, method_name: str):
        super().__init__(method_name)
        self.params = ConnectionParameters()

    def assertEmpty(self, obj):
        self.assertFalse(obj)

    def assertNotEmpty(self, obj):
        self.assertTrue(obj)
