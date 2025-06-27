from unittest import TestCase

from database import Database, PrintableResult


class TestDatabase(TestCase):

    def test_execute_non_transacted(self):
        database = Database()
        e, s = database.execute, database.commited_state

        self.assertEqual(e('GET A'), PrintableResult(None))

        e('SET A 10')
        self.assertEqual(s, {'A': 10})
        self.assertEqual(e('GET A'), PrintableResult(10))

        self.assertEqual(e('COUNTS 10'), PrintableResult(1))
        e('SET B 20')
        e('SET C 10')
        self.assertEqual(e('COUNTS 10'), PrintableResult(2))

        e('UNSET B')
        self.assertEqual(e('GET B'), PrintableResult(None))

    def test_execute_transacted(self):
        database = Database()
        e = database.execute

        e('BEGIN')

        self.assertFalse(database.transactions)
        self.assertTrue(database.in_transaction)

        e('SET A 10')

        self.assertTrue(database.transactions)

        e('BEGIN')
        e('SET A 20')
        e('BEGIN')
        e('SET A 30')
        self.assertEqual(e('GET A'), PrintableResult(30))
        e('ROLLBACK')
        self.assertEqual(e('GET A'), PrintableResult(20))
        e('COMMIT')
        self.assertEqual(e('GET A'), PrintableResult(20))
        self.assertEqual(database.commited_state, {'A': 20})
        self.assertFalse(database.transactions)
        self.assertFalse(database.in_transaction)

    def test_find(self):
        database = Database()
        e = database.execute
        e('SET A 10')
        e('SET B 10')
        self.assertEqual(e('FIND 10'), PrintableResult('A B'))

    def test_end(self):
        database = Database()
        with self.assertRaises(EOFError):
            database.execute('END')
