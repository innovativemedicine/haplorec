package haplorec.test.util

import haplorec.util.Row

class RowTest extends GroovyTestCase {

    def rows(iter) {
        def xs = []
        iter.each { xs.add it }
        xs
    }

    def noDuplicatesTest(iter, groups, expectedRows) {
        assertEquals( expectedRows , rows( Row.noDuplicates(iter, groups) ) )
    }

    void testNoDuplicates() {
        noDuplicatesTest(
            [
                [a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7],
                [a: 1, b: 2, c: 3, d: 4, e: 5, f: 7, g: 8],
                [a: 1, b: 2, c: 3, d: 4, e: 5, f: 8, g: 9],
                [a: 3, b: 4, c: 3, d: 4, e: 5, f: 6, g: 7],
                [a: 3, b: 4, c: 3, d: 4, e: 5, f: 7, g: 8],
                [a: 3, b: 4, c: 3, d: 4, e: 5, f: 8, g: 9],
            ],
            [
                A: [['a'], ['a', 'b', 'c']],
                B: [['c'], ['c', 'd', 'e']],
                C: [['e', 'f'], ['e', 'f', 'g']],
            ],
            [
                [a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7],
                [                        e: 5, f: 7, g: 8],
                [                        e: 5, f: 8, g: 9],
                [a: 3, b: 4, c: 3                        ],
                [:],
                [:],
            ])
    }

    def collapseTest(Map kwargs = [:], iter, expectedRows) {
        assertEquals( expectedRows , rows( Row.collapse(kwargs, iter) ) )
    }

    void testCollapse() {

        collapseTest(
            [
                [a: 1, b: 2, c: 3],
                [            c: 3],
                [a: 1, b: 2      ],
            ],
            [
                [a: 1, b: 2, c: 3],
                [a: 1, b: 2, c: 3],
            ])

        collapseTest(
            [
                [a: 1, b: 2, c: 3],
                [            c: 3],
                [a: 1, b: 2, c: null],
            ],
            [
                [a: 1, b: 2, c: 3],
                [            c: 3],
                [a: 1, b: 2, c: null],
            ])

        def nonNullKeys = { map -> 
            map.keySet().grep { k -> map[k] != null } 
        }

        collapseTest(
            canCollapse: { header, lastRow, currentRow ->
                // 2.
                def intersect = new HashSet(nonNullKeys(lastRow))
                intersect.retainAll(nonNullKeys(currentRow))
                return intersect.size() == 0
            },
            collapse: { header, lastRow, currentRow ->
                header.each { h ->
                    if (lastRow[h] == null) {
                        lastRow[h] = currentRow[h]
                    }
                }
            },
            [
                [a: 1, b: 2, c: 3],
                [            c: 3],
                [a: 1, b: 2, c: null],
            ],
            [
                [a: 1, b: 2, c: 3],
                [a: 1, b: 2, c: 3],
            ])


        collapseTest(
            canCollapse: { header, lastRow, currentRow ->
                // 2.
                def intersect = new HashSet(nonNullKeys(lastRow))
                intersect.retainAll(nonNullKeys(currentRow))
                return intersect.size() == 0
            },
            collapse: { header, lastRow, currentRow ->
                header.each { h ->
                    if (lastRow[h] == null) {
                        lastRow[h] = currentRow[h]
                    }
                }
            },
            [
                [a: 1, b: 2, c: 3],
                [a: 1, b: 2, c: null],
                [            c: 3],
            ],
            [
                [a: 1, b: 2, c: 3],
                [a: 1, b: 2, c: 3],
            ])

    }

}
