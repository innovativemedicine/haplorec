#!/usr/bin/env python
import unittest
from testutil import DatabaseTestMixin, tmp_dir, create_files, rows_to_dsv
import oursql
from load_dsv import *
import os.path

class test_load_dsv(unittest.TestCase, DatabaseTestMixin):
    db = 'load_dsv_test'
    schema_file = "src/sql/mysql/haplorec.sql"

    drop_after_test = True

    def tearDown(self):
        if self.drop_after_test:
            self.drop()

    def load_dsv_test(self, schema=None, files=None, assertion=None, *args, **kwargs):
        self.schema = schema 
        self.connect()
        with tmp_dir() as directory:
            def filepath(f):
                return os.path.join(directory, f)
            files_to_create = {}
            for filename, rows in files:
                files_to_create[filename] = rows_to_dsv(rows)
            create_files(files_to_create, directory=directory)
            load_dsv(self.connection, [filepath(f[0]) for f in files], *args, delim="\t", **kwargs)
            assertion()

    def test_dump(self):
        """
        Test using tables with no foreign keys (i.e. just dump the csv files in).
        """
        def assertion():
            self.assertEqual(
                self.select(table='T', columns=["x", "y"]), 
                [
                    ( "x1", "y1" ),
                    ( "x2", "y2" ),
                ]
            )
            self.assertEqual(
                self.select(table='R_1', columns=["z",  "x"]), 
                [
                    ( "z1", "x1" ),
                ]
            )
        self.load_dsv_test(
            schema="""
                CREATE TABLE T ( x text, y text );
                CREATE TABLE R_1 ( z text, x text );
            """,
            files=[
                ['T.dsv', [
                    [ "x",  "y" ],
                    [ "x1", "y1" ],
                    [ "x2", "y2" ],
                ]],
                ['R_1.dsv', [
                    [ "z",  "x" ],
                    [ "z1", "x1" ],
                ]],
            ],
            assertion=assertion,
        )

    def test_ignore(self):
        """
        Test ignoring the insertion of certain fields.
        """
        self.drop_after_test = False

    def test_id(self):
        """
        Test mapping back to a single auto_increment table.
        """
        self.drop_after_test = False

if __name__ == '__main__':
    unittest.main()
