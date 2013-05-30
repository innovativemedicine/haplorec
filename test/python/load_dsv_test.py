#!/usr/bin/env python
import unittest
from testutil import DatabaseTestMixin, tmp_dir, create_files, rows_to_dsv
import oursql
from load_dsv import *

class test_load_dsv(unittest.TestCase, DatabaseTestMixin):
    db = 'haplorec_testing'
    schema_file = "src/sql/mysql/haplorec.sql"

    drop_after_test = True

    # def __init__(self):
    #     unittest.TestCase.__init__(self)
    #     DatabaseTest.__init__(self, db='haplorec_testing', schema_file="src/sql/mysql/haplorec.sql")

    def setUp(self):
        self.connect()

    def tearDown(self):
        if self.drop_after_test:
            self.drop()

    def test_dump(self):
        """
        Test using tables with no foreign keys (i.e. just dump the csv files in).
        """
        with tmp_dir() as directory:
            create_files({
                'T.dsv': rows_to_dsv([
                    [ "x",  "y" ],
                    [ "x1", "y1" ],
                    [ "x2", "y2" ],
                ]),
                'R_1.dsv': rows_to_dsv([
                    [ "z",  "x" ],
                    [ "z1", "x1" ],
                ]),
            }, directory=directory)
            load_dsv(self.connection, ['T.dsv', 'R_1.dsv'], delim="\t")
            import pdb; pdb.set_trace()

    # def test_ignore(self):
    #     """
    #     Test ignoring the insertion of certain fields.
    #     """
    #     pass

    # def test_id(self):
    #     """
    #     Test mapping back to a single auto_increment table.
    #     """
    #     pass

if __name__ == '__main__':
    unittest.main()
