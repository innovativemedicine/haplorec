package haplorec.util.dependency

import groovy.transform.EqualsAndHashCode

@EqualsAndHashCode(includes='target')
class Dependency {
	String target
	/* Given that all the rules for dependencies that this dependency dependsOn have been run (hence, built), build this dependency.
	 */
	def rule
	/* TODO: () -> (); executed when this dependency has been built, and is no longer required by any remaining dependencies to be built that depend on it
	 * this is just an optimization and is probably not worth the effort
	 */
	def finished
	List<Dependency> dependsOn = []
	
	/*
def dep_resolve(node, resolved, seen):
   print node.name
   seen.append(node)
   for edge in node.edges:
          if edge not in resolved:
                 if edge in seen:
                        raise Exception('Circular reference detected: %s -&gt; %s' % (node.name, edge.name))
                 dep_resolve(edge, resolved, seen)
   resolved.append(node)
	 */
//	void build() {
		/* TODO:
		 * implement http://www.electricmonk.nl/docs/dependency_resolving_algorithm/dependency_resolving_algorithm.html#_representing_the_data_graphs
		 */
//		build(this, new HashSet<Dependency>(), new HashSet<Dependency>())
//	}
	
	void build(Set<Dependency> built = new HashSet<Dependency>()) {
		/* TODO:
		 * implement http://www.electricmonk.nl/docs/dependency_resolving_algorithm/dependency_resolving_algorithm.html#_representing_the_data_graphs
		 */
		bld(this, built, new HashSet<Dependency>(built))
	}
	
	private static bld(Dependency d, Set<Dependency> built, Set<Dependency> seen) {
		seen.add(d)
		d.dependsOn.each { dependency ->
			if (!built.contains(dependency)) {
				if (seen.contains(dependency)) {
					throw new RuntimeException("Circular reference detected: ${target.target} -> ${dependency.target}")
				}
				bld(dependency, built, seen)
			}
		}
		built.add(d)
		d.rule()
		// TODO: does any still depend on me and need to be built?  If not, call
	}
	
	@Override
	public String toString() {
		return (dependsOn.size() == 0) ?
			target :
			"$target <- $dependsOn"
	}
}
