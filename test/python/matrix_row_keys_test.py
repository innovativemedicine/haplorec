#!/usr/bin/env python
import unittest
from matrix_row_keys import *

def row_names(n):
    return ['y' + str(i) for i in xrange(1, n+1)]
def column_names(n):
    return ['x' + str(i) for i in xrange(1, n+1)]

class test_matrix_row_keys(unittest.TestCase):
    def _matrix_row_keys_test(self, expect, *args, **kwargs):
        self.assertEqual(matrix_row_keys(*args, **kwargs), expect)

    def test_basic(self):
        """
        Simple 1x1 matrix.
        """
        self.assertEqual(
            matrix_row_keys(column_names(1), row_names(1), [[1]]), 
            {
                'y1': set([
                    frozenset([('x1', 1)]),
                ])
            }
        )

    def test_single_keys(self):
        """
        5x4 matrix where each row is uniquely determined by one key.
        """
        self.assertEqual(
            matrix_row_keys(column_names(4), row_names(5), [
                [1, 2, 3, 4],
                [2, 2, 3, 4],
                [1, 2, 3, 5],
                [2, 2, 3, 5],
                [2, 3, 3, 5],
            ]), 
            {
                'y1': set([
                    frozenset([('x1', 1), ('x4', 4)]),
                ]),
                'y2': set([
                    frozenset([('x1', 2), ('x4', 4)]),
                ]),
                'y3': set([
                    frozenset([('x1', 1), ('x4', 5)]),
                ]),
                'y4': set([
                    frozenset([('x1', 2), ('x2', 2), ('x4', 5)]),
                ]),
                'y5': set([
                    frozenset([('x2', 3)]),
                ]),
            }
        )

    def test_multiple_keys(self):
        """
        3x4 matrix where each row is uniquely determined by multiple keys.
        """
        self.assertEqual(
            matrix_row_keys(column_names(4), row_names(3), [
                [1, 2, 1, 2],
                [1, 2, 3, 4],
                [3, 4, 1, 2],
            ]), 
            {
                # TODO:
                'y1': set([
                    frozenset([('x1', 1), ('x3', 1)]),
                    frozenset([('x2', 2), ('x4', 2)]),
                    frozenset([('x2', 2), ('x3', 1)]),
                    frozenset([('x1', 1), ('x4', 2)]),
                ]),
                'y2': set([
                    frozenset([('x3', 3)]),
                    frozenset([('x4', 4)]),
                ]),
                'y3': set([
                    frozenset([('x1', 3)]),
                    frozenset([('x2', 4)]),
                ]),
            }
        )

    def test_mutually_exclusive_keys(self):
        """
        3x3 matrix where the first row has one key of size 2, and another of size 1 (but both keys 
        are mutually exclusive in their columns used).
        """
        self.assertEqual(
            matrix_row_keys(column_names(3), row_names(3), [
                [1, 1, 1],
                [2, 1, 2],
                [3, 3, 1],
            ]), 
            {
                # TODO:
                'y1': set([
                    frozenset([('x1', 1)]),
                    frozenset([('x2', 1), ('x3', 1)]),
                ]),
                'y2': set([
                    frozenset([('x1', 2)]),
                    frozenset([('x3', 2)]),
                ]),
                'y3': set([
                    frozenset([('x1', 3)]),
                    frozenset([('x2', 3)]),
                ]),
            }
        )



if __name__ == '__main__':
    unittest.main()
