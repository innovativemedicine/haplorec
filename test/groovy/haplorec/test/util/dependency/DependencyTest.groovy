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

}
