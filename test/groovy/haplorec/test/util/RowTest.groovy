package haplorec.test.util

import haplorec.util.Row

class RowTest extends GroovyTestCase {

    def noDuplicatesTest(iter, groups, expectedRows) {
        def collect = { iterator ->
            def xs = []
            iterator.each { xs.add it }
            xs
        }
        assertEquals( expectedRows , collect( Row.noDuplicates(iter, groups) ) )
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

}
