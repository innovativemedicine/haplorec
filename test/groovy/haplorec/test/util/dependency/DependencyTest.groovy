package haplorec.test.util.dependency

import haplorec.util.dependency.Dependency
import haplorec.util.dependency.DependencyGraphBuilder

class DependencyTest extends GroovyTestCase {

    def builder = new DependencyGraphBuilder()
    def targetAdder(List<Dependency> buildOrder) {
		return {
			t -> buildOrder.add(t)
		}
    }

	void setUp() {
	}

	void tearDown() {
	}

    def assertBuildOrder(List<Set<Dependency>> expectedBuildOrder, List<Dependency> buildOrder) {
		log.info("expectedBuildOrder == $expectedBuildOrder, buildOrder == $buildOrder")
        int level = 0
        Set<Dependency> seen = [] as Set
        buildOrder.each { d ->
			seen.add(d)
			assert level < expectedBuildOrder.size() : "testcase has specified the expectedBuildOrder ($expectedBuildOrder) correctly"
			if (!expectedBuildOrder[level].contains(d)) {
				fail("Saw dependency $d to be built at level $level, but that level only contains ${expectedBuildOrder[level]}; buildOrder == $buildOrder")
			}
            if (seen == expectedBuildOrder[level]) {
                level += 1
                seen.clear()
            }
        }
    }

    void testSingleAndDoubleDependants() {
		def buildOrder = []
		def expectedBuildOrder = [
            ['A'],
            ['B'],
			['C'],
            ['D'],
			['E'],
        ].collect { it as Set }
		def addTarget = targetAdder(buildOrder)
		Dependency A, B, C, D, E
		E = builder.dependency(id: 'E', target: 'E', rule: { -> addTarget('E') }) {
			C = dependency(id: 'C', target: 'C', rule: { -> addTarget('C') }) {
				B = dependency(id: 'B', target: 'B', rule: { -> addTarget('B') }) {
					A = dependency(id: 'A', target: 'A', rule: { -> addTarget('A') })
				}
			}
			D = dependency(id: 'D', target: 'D', rule: { -> addTarget('D') }) {
				dependency(refId: 'C')
			}
		}
		def testTarget = { target, built = [], expctedBuildOrder = expectedBuildOrder ->
			buildOrder.clear()
			target.build(built as Set)
			assertBuildOrder(expctedBuildOrder, buildOrder)
		}
		
        testTarget(A)
        testTarget(B)
        testTarget(C)
        testTarget(D)
        testTarget(E)
		
		testTarget(E, [C, D], [
			['E'],
        ].collect { it as Set })
		
		testTarget(E, [D], [
			['A'],
			['B'],
			['C'],
			['E'],
		].collect { it as Set })
		
    }
	
	void testLowerThenHigherLevelVisit() {
		/* Makes sure nodes that get visited at a lower level first, then a higher level retain the lower level.  In particular, B gets visited first from A (level 0), and then from C (level 1), so we expect B as level 1. 
		 */
        def builder = new DependencyGraphBuilder()
		def _ = { -> }
		def dep = { name -> [id:name, target:name, rule:_] }
		Dependency A, B, C, D, E, F
		A = builder.dependency(dep('A')) {
            B = dependency(dep('B')) {
                D = dependency(dep('D')) {
                }
                E = dependency(dep('E')) {
                }
            }
            C = dependency(dep('C')) {
                B = dependency(refId: 'B')
                F = dependency(dep('F')) {
                }
            }
        }
        def lvls = A.levels()
        assert A.dependsOn == [B, C] && C.dependsOn == [B, F]: "traversal order to reach B is as expected in test behaviour"
        assert lvls == [
            (A): 0,
            (B): 1,
            (C): 1,
            (D): 2,
            (E): 2,
            (F): 2,
        ]
	}


	void testHigherThenLowerLevelVisit() {
		/* Makes sure nodes that get visited at a higher level first, then a lower level retain the higher level.  In particular, B gets visited first from C (level 1), and then from A (level 0), so we expect B as level 1. 
		 */
        def builder = new DependencyGraphBuilder()
		def _ = { -> }
		def dep = { name -> [id:name, target:name, rule:_] }
		Dependency A, B, C, D, E, F
		A = builder.dependency(dep('A')) {
            C = dependency(dep('C')) {
                B = dependency(dep('B')) {
                    D = dependency(dep('D')) {
                    }
                    E = dependency(dep('E')) {
                    }
                }
                F = dependency(dep('F')) {
                }
            }
            B = dependency(refId: 'B')
        }
        def lvls = A.levels()
        assert A.dependsOn == [C, B] && C.dependsOn == [B, F]: "traversal order to reach B is as expected in test behaviour"
        assert lvls == [
            (A): 0,
            (B): 1,
            (C): 1,
            (D): 2,
            (E): 2,
            (F): 2,
        ]
	}


	void testMultipleStartingNodes() {
		/* Makes sure nodes that get visited at a higher level first, then a lower level retain the higher level.  In particular, B gets visited first from C (level 1), and then from A (level 0), so we expect B as level 1. 
		 */
        def builder = new DependencyGraphBuilder()
		def _ = { -> }
		def dep = { name -> [id:name, target:name, rule:_] }
		Dependency A, B, C, D, E, F, G
		A = builder.dependency(dep('A')) {
            C = dependency(dep('C')) {
                B = dependency(dep('B')) {
                    D = dependency(dep('D')) {
                    }
                    E = dependency(dep('E')) {
                    }
                }
                F = dependency(dep('F')) {
                }
            }
            B = dependency(refId: 'B')
        }
		G = builder.dependency(dep('G')) {
            F = dependency(refId: 'F')
        }

        def lvls = A.levels(startAt: [A, G])
        assert A.dependsOn == [C, B] && C.dependsOn == [B, F]: "traversal order to reach B is as expected in test behaviour"
        assert lvls == [
            (A): 0,
            (B): 1,
            (C): 1,
            (D): 2,
            (E): 2,
            (F): 1,
            (G): 0,
        ]
        // order of dependencies in startAt should be irrelevant
        def lvlsAgain = A.levels(startAt: [G, A])
        assert lvls == lvlsAgain

    }

}
