package haplorec.util.dependency

import groovy.util.ObjectGraphBuilder
import groovy.util.ObjectGraphBuilder.ChildPropertySetter;
import groovy.util.ObjectGraphBuilder.IdentifierResolver

/** Provides a DSL for creating a dependency graph from the Dependency class, where we represent the 
 * graph by having a target reference its dependencies (through its dependsOn property).
 *
 * Given a dependency graph defined with these relationships (NOTE: A -> B means A depends on B):
 * E -> C
 * C -> B
 * B -> A
 * E -> D
 * D -> C
 *
 * We can build such a dependency graph using this class by doing:
 * 
 * Dependency A, B, C, D, E
 * def builder = new DependencyGraphBuilder()
 * E = builder.dependency(id: 'E', target: 'E') {
 *     C = dependency(id: 'C', target: 'C') {
 *         B = dependency(id: 'B', target: 'B') {
 *             A = dependency(id: 'A', target: 'A')
 *         }
 *     }
 *     D = dependency(id: 'D', target: 'D') {
 *         // we already declared C above; use refId to reference it
 *         dependency(refId: 'C')
 *     }
 * }
 *
 * We could've added the "rule" definitions when declaring dependency(...), but it gets messy, so we 
 * can instead do it separately:
 * A.rule = { ->
 *     // create A
 * }
 *
 * Based on http://groovy.codehaus.org/ObjectGraphBuilder
 */
class DependencyGraphBuilder extends ObjectGraphBuilder {
	
	public DependencyGraphBuilder() {
		super()
		this.classNameResolver = "haplorec.util.dependency"
		this.childPropertySetter = new DependencyChildPropertySetter()
		this.newInstanceResolver = { klass, attributes -> 
			klass.newInstance(attributes) 
		}
	}

	private static class DependencyChildPropertySetter implements ChildPropertySetter {
		void setChild(Object parent, Object child, String parentName, String propertyName) {
			parent.dependsOn.add(child)
		}
	}
	
	private static class DependencyIdentifierResolver implements IdentifierResolver {
		String getIdentifierFor(String arg0) {
			return "target"
		}
	}
	
}
