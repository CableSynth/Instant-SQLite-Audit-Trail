#!/usr/bin/env python3

import sqlite3
import unittest

import audit


class TestAudit(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')

        self.conn.execute('CREATE TABLE tab(c1, c2)')
        audit.attach_log(self.conn)

    def tearDown(self):
        audit.detach_log(self.conn)
        self.conn.close()

    def test_string_to_python(self):
        self.conn.execute("INSERT INTO tab VALUES('a', 'b')")

        r = self.conn.execute("SELECT * FROM _audit").fetchone()
        py_val = audit.to_python(r[4])

        self.assertEqual(py_val,
                         [['c1', 'a'],
                          ['c2', 'b']])

    def test_nums_to_python(self):
        self.conn.execute("INSERT INTO tab VALUES(5, 3.14)")

        r = self.conn.execute("SELECT * FROM _audit").fetchone()
        py_val = audit.to_python(r[4])

        self.assertEqual(py_val,
                         [['c1', 5],
                          ['c2', 3.14]])

    def test_null_to_python(self):
        self.conn.execute("INSERT INTO tab VALUES(NULL, NULL)")

        r = self.conn.execute("SELECT * FROM _audit").fetchone()
        py_val = audit.to_python(r[4])

        self.assertEqual(py_val,
                         [['c1', None],
                          ['c2', None]])

    def test_insert(self):
        self.conn.execute("INSERT INTO tab VALUES('audit', 'this')")

        audit_rows = self.conn.execute("SELECT * FROM _audit").fetchall()
        self.assertEqual(len(audit_rows), 1)

        r = audit_rows[0]

        #table and op
        self.assertEqual(r[1:3], (u'tab', u'INSERT'))

        #no previous, new is what we inserted
        self.assertEqual(r[3:],
                         (None,
                          str([['c1', 'audit'],
                                        ['c2', 'this']]),
                         ))

    def test_update(self):
        self.conn.execute("INSERT INTO tab VALUES('audit', 'this')")
        self.assertEqual(
            self.conn.execute("UPDATE tab SET c2='everything' WHERE"
                              " c2='this'").rowcount,
            1)

        audit_rows = self.conn.execute("SELECT * FROM _audit").fetchall()
        self.assertEqual(len(audit_rows), 2)

        r = audit_rows[1]

        self.assertEqual(r[1:3], (u'tab', u'UPDATE'))
        self.assertEqual(r[3:],
                         (str([['c1', 'audit'],
                                        ['c2', 'this']]),
                          str([['c1', 'audit'],
                                        ['c2', 'everything']]),
                         ))

    def test_delete(self):
        self.conn.execute("INSERT INTO tab VALUES('audit', 'this')")
        self.assertEqual(
            self.conn.execute("DELETE FROM tab WHERE c1='audit'").rowcount,
            1)

        audit_rows = self.conn.execute("SELECT * FROM _audit").fetchall()
        self.assertEqual(len(audit_rows), 2)

        r = audit_rows[1]

        self.assertEqual(r[1:3], (u'tab', u'DELETE'))
        self.assertEqual(r[3:],
                         (str([['c1', 'audit'],
                                        ['c2', 'this']]),
                          None,
                         ))

    def test_update_null(self):
        self.conn.execute("INSERT INTO tab VALUES('audit', NULL)")
        self.assertEqual(
            self.conn.execute("UPDATE tab SET c2='everything' WHERE"
                              " c2 is NULL").rowcount,
            1)

        audit_rows = self.conn.execute("SELECT * FROM _audit").fetchall()
        self.assertEqual(len(audit_rows), 2)

        r = audit_rows[1]

        self.assertEqual(r[1:3], (u'tab', u'UPDATE'))
        self.assertEqual(r[3:],
                         (str([['c1', 'audit'],
                                        ['c2', None]]),
                          str([['c1', 'audit'],
                                        ['c2', 'everything']]),
                         ))

    def test_detach(self):
        audit.detach_log(self.conn)

        self.conn.execute("INSERT INTO tab VALUES('no', 'audit')")

        with self.assertRaises(sqlite3.OperationalError):
            #no _audit table
            self.conn.execute("SELECT * FROM _audit").fetchall()


if __name__ == '__main__':
    unittest.main()
