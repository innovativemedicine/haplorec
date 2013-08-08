package haplorec.test.util.dependency

import haplorec.util.dependency.Dependency
import haplorec.util.dependency.DependencyGraphBuilder
import groovy.transform.InheritConstructors

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
        def lvls = Dependency.levels([A, B, C, D, E, F])
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
        def lvls = Dependency.levels([A, B, C, D, E, F])
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

        def lvls = Dependency.levels([A, B, C, D, E, F, G])
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

    }

    def buildHandlerTest(Closure addHandler) {
        /* Test that build handlers:
         * 1. all get called
         * 2. get called in the order they were added
         * 3. get called in proper dependency order
         */
        def state = []
        def buildHandler = { i ->
            { dependency ->
                state.add(dependency.target + i)
            }
        }
        def builder = new DependencyGraphBuilder()
		def _ = { -> }
		def dep = { name -> [id:name, target:name, rule:_] }
		Dependency A, B, C
		C = builder.dependency(dep('C')) {
            B = dependency(dep('B')) {
                A = dependency(dep('A')) 
            }
        }
        def numHandlers = 2
        [A, B, C].each { d ->
            (1..numHandlers).each { i ->
                addHandler(d, buildHandler(i))
            }
        }
        def expect = [A, B, C].collect { d -> 
            (1..numHandlers).collect { i -> d.target + i } 
        }.flatten()
        C.build()
        def got = state 
        assert expect == got
    }

    void testAfterBuild() {
        buildHandlerTest() { d, handler ->
            d.afterBuild += handler
        }
    }

    void testBeforeBuild() {
        buildHandlerTest() { d, handler ->
            d.beforeBuild += handler
        }
    }

	@InheritConstructors
	static class OnFailException extends RuntimeException {}
    /* expectedOnFail: the dependencies whose onFail handlers we expect to get fired.
     */
    def onFailTest(Map kwargs = [:], expectedOnFail, expectedBeforeBuild) {
        /* Test that build handlers:
         * 1. all get called
         * 2. get called in the order they were added
         * 3. get called in proper dependency order
         */
        if (kwargs.propagateFailure == null) { kwargs.propagateFailure = true }
        if (kwargs.fail == null) { kwargs.fail = [] as Set }
        def onFailState = []
        def onFailHandler = { i ->
            { dependency, exception ->
                onFailState.add(dependency.target + i)
            }
        }
        def beforeBuildState = []
        def beforeBuildHandler = { i ->
            { dependency ->
                beforeBuildState.add(dependency.target + i)
            }
        }
        def builder = new DependencyGraphBuilder()
        def rule = { target ->
            { ->
                if (target in kwargs.fail) {
                    throw new OnFailException(target)
                }
            }
        }
		def dep = { name -> [id:name, target:name, rule:rule(name), propagateFailure: kwargs.propagateFailure] }
		Dependency A, B, C
		C = builder.dependency(dep('C')) {
            B = dependency(dep('B')) {
                A = dependency(dep('A')) 
            }
        }
        def numHandlers = 2
        [A, B, C].each { d ->
            (1..numHandlers).each { i ->
                d.onFail += onFailHandler(i)
                d.beforeBuild += beforeBuildHandler(i)
            }
        }
        def deps = [A, B, C].inject([:]) { m, d -> m[d.target] = d; m }
        def expected = { xs ->
            xs.collect { target -> 
                (1..numHandlers).collect { i -> target + i } 
            }.flatten()
        }
        if (kwargs.propagateFailure) {
            shouldFail(OnFailException) {
                C.build()
            }
        } else {
            C.build()
        }

        def gotOnFail = onFailState 
        def expectOnFail = expected(expectedOnFail)
        assert expectOnFail == gotOnFail

        def gotBeforeBuild = beforeBuildState 
        def expectBeforeBuild = expected(expectedBeforeBuild)
        assert expectBeforeBuild == gotBeforeBuild
    }

    void testOnFailWithPropagation() {
        onFailTest(['A'], ['A'],
            propagateFailure: true,
            fail: ['A'] as Set)
        onFailTest(['B'], ['A', 'B'],
            propagateFailure: true,
            fail: ['B'] as Set)
        onFailTest(['C'], ['A', 'B', 'C'],
            propagateFailure: true,
            fail: ['C'] as Set)
		onFailTest(['A'], ['A'],
			propagateFailure: true,
			fail: ['A', 'B', 'C'] as Set)
    }

}
