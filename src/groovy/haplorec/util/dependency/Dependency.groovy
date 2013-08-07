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
    List<Closure> beforeBuild = []
    List<Closure> afterBuild = []
    List<Closure> onFail = []
    Boolean propagateFailure = true
	
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
        d.beforeBuild.each { handler ->
            handler(d)
        }
        try {
            d.rule()
        } catch (Exception e) {
            d.onFail.each { handler ->
                handler(d, e)
            }
            if (d.propagateFailure) {
                throw e
            }
        }
        d.afterBuild.each { handler ->
            handler(d)
        }
		// TODO: does any still depend on me and need to be built?  If not, call
	}
	
	@Override
	public String toString() {
        return target
		// return (dependsOn.size() == 0) ?
		// 	target :
		// 	"$target <- $dependsOn"
	}
	
	private static lvls(Integer l, Dependency d, Map<Dependency, Integer> lvl) {
		if (!lvl.containsKey(d)) {
			lvl[d] = l
		} else if (l < lvl[d]) {
			lvl[d] = l 
		}
		d.dependsOn.each { dependency ->
			if (!lvl.containsKey(dependency) || l + 1 < lvl[dependency]) {
				// if we haven't visited or its smaller
				lvls(l + 1, dependency, lvl)
			}
		}
	}
	
	Map<Dependency, Integer> levels(Map kwargs = [:]) {
		if (kwargs.start == null) { kwargs.start = 0 }
        if (kwargs.levels == null) { kwargs.levels = new HashMap() }
        if (kwargs.startAt == null) { 
            kwargs.startAt = [this]
        }
        kwargs.startAt.each { d ->
            lvls(kwargs.start, d, kwargs.levels)
        }
		return kwargs.levels
	}

    /* Given a collection (it should really be a Set) of dependencies in a dependency graph, return 
     * a map from Dependency d to it's dependants (i.e. { x | x.dependsOn contains d } ).
     */
    static Map<Dependency, Set<Dependency>> dependants(Collection<Dependency> dependencies) {
        Map D = dependencies.inject([:]) { m, d ->
            m[d] = [] as Set
            return m
        }
        dependencies.each { dependant ->
            dependant.dependsOn.each { dependency ->
                D.get(dependency, [] as Set).add(dependant)
            }
        }
        return D
    }
	private static transitiveDep(depList){
		if (depList!=[]){
			for (i in depList){
				for (j in i.dependants){
					j.rowLevel=i.rowLevel+1
					j.group=i.group
				transitiveDep(i.dependants)
				}
			}
		}
	}
	
	private static rowLvls(columnLevel,depList){
		def levelList=[]
		/* filter depList into
		 * levelList, a list of targets with the inputed column Level
		 */
		for (i in depList){
			if (i.columnLevel == columnLevel){
				levelList+=i
			}
		}
		/* filter out any targets that are not in the column Level
		 * from each target's dependsOn list and dependants list
		 * if dependsOn==[] then add to nulldepOnList
		 */
		def nulldepOnList=[]
		for (i in levelList){
			//change dependsOn really a list of targets not names
			i.dependsOn=i.dependsOn.findAll{it in levelList.collect{it.name}}
			i.dependants=i.dependants.findAll{it in levelList}
			if (i.dependsOn==[]){
				nulldepOnList+=i
			}
		}
		/* Put nulldepOnList in alphabetical order
		 */
		nulldepOnList.sort{it.name}
		/* Assign group and rowLevel to nulldepOnList
		 */
		for (i in nulldepOnList){
			i.group=nulldepOnList.indexOf(i)
			i.rowLevel=0
		}
		/* Assign transitive dependencies to each target
		 */
		transitiveDep(nulldepOnList)
	
		/* Find the number of groups in the level by finding the
		 * maximum of it.group in levelList
		 */
		def numOfGroups=levelList.collect{it.group}.max()+1
	
		/* List of lists, each list has all the targets with the same group level
		 */
		def grouplist=[]
	
		for (int i=0; i < numOfGroups;i++){
			grouplist+=[depList.findAll{it.group==i}]
		}
		def rowLevelList=[]
		/* Sort each list in group list by rowLevel and then by alpha
		 * then join list to rowLevelList
		 */
		for (i in grouplist){
			i.sort{x,y->
				if (x.rowLevel==y.rowLevel){
					x.name <=> y.name
				}else{
					x.rowLevel <=> y.rowLevel
				}
			}
			rowLevelList+=i
		}
	
		return rowLevelList
	}

}
