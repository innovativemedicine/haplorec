package haplorec.util.dependency

import groovy.transform.EqualsAndHashCode

/** This class represents a dependency in a dependency graph, and its static methods represent 
 * associated algorithms on dependency graphs.
 * 
 * Given a dependency graph defined with these relationships (NOTE: A -> B means A depends on B):
 * A -> B
 * A -> C
 *
 * Then A looks like:
 * Dependency(target:'A', dependsOn: [
 *     Dependency(target:'B', dependsOn:[]), 
 *     Dependency(target:'C', dependsOn:[]),
 * ])
 *
 * Rules for building a dependency are specified via the rule property, which is a function of type 
 * ( -> ) (i.e. no arguments or return values).
 *
 * Terminology:
 * - transitive dependencies: 
 *   the transitive dependencies of d are the direct dependencies of d, union all the transitive 
 *   dependencies of d's direct dependencies (i.e. all the nodes we'd have to build if we built d).
 * - dependant: 
 *   if B is a dependency of A, then A is a dependant of B.
 */
@EqualsAndHashCode(includes='target')
class Dependency {
    /* A unique name identifying this target.
     */
	String target
    /* A function of type ( -> ) (i.e. no arguments or return values) that builds this target 
     * (assumes all the all the rules for dependencies that this dependency dependsOn have been run).
	 */
	def rule
	List<Dependency> dependsOn = []
    /* Handlers to be executed at different points in the process of building this target:
     * - beforeBuild: ( Dependency -> )
     *   called immediately before calling rule, but after all of its depdenencies have been built
     *
     * - afterBuild: ( Dependency -> )
     *   called immediately after calling rule, but it is only called when rule didn't throw an 
     *   exception or propagateFailure is false
     *
     * - onFail: ( Dependency, Exception -> ) 
     *   called immediately after rule throws an Exception
     */
    List<Closure> beforeBuild = []
    List<Closure> afterBuild = []
    List<Closure> onFail = []
    /* Whether or not to re-throw an exception after it is passed to onFail handlers.
     */
    Boolean propagateFailure = true

	@Override
	public String toString() {
        return target
	}

    /** Algorithms.
     * =============================================================================================
     * - Building a target and its associated dependencies is implemented via the bld method 
     * - Layout algorithms for arranging the dependencies of a graph in a cartesian coordinate system 
     *   are implemented in levels and rowLevels 
     * - Calculation of dependants (using dependency relationships) is implemented in dependants
     */
	
    /** Build this target (not the entire graph).
     *
     * @param built
     * the targets that have already been built (default: empty set)
     */
	void build(Set<Dependency> built = new HashSet<Dependency>()) {
        /* Targets that we have visited (used to detect circular dependencies)
         */
        Set<Dependency> seen = new HashSet<Dependency>(built)
        /** Build d, which entails building d's transitive dependencies first (unless they're already 
         * been built, according to their precense in 'built').
         *
         * @param d
         * the target to build
         * @param seen
         *
         * Source: http://www.electricmonk.nl/docs/dependency_resolving_algorithm/dependency_resolving_algorithm.html#_representing_the_data_graphs
         */
        def bld
        bld = { Dependency d ->
            seen.add(d)
            d.dependsOn.each { dependency ->
                if (!built.contains(dependency)) {
                    /* If we've already seen this dependency, 
                    */
                    if (seen.contains(dependency)) {
                        throw new RuntimeException("Circular reference detected: ${target.target} -> ${dependency.target}")
                    }
                    bld(dependency)
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
        }
		bld(this)
	}

    /** Build all dependencies in the graph.
     */
    static void buildGraph(Collection<Dependency> dependencies) {
        Set<Dependency> built = []
        noDependants(dependencies).each { d ->
            d.build(built)
        }
    }

    /** Calculate the level of dependencies in the graph, defined to be the shortest path from d to 
     * a target having no dependants (the "leaves" of a dependency graph).
     *
     * @param dependencies
     * the set of all dependencies in a dependency graph
     */
	static Map<Dependency, Integer> levels(Collection<Dependency> dependencies) {
        /* A mapping from Dependency -> Integer, representing the shortest path length from that 
         * dependency to a target with no dependants.
         */
        Map<Dependency, Integer> lvl = [:]
        /** For each transitive dependency t of d, record in lvl[t] the length of the shortest path from 
         * t to d.
         *
         * Typically, you'd initiate this algorithm with:
         * d = a target with no dependants
         * l = 0
         *
         * @param l
         * the length of a path from d to a target with no dependants (travelling along dependants).
         * @param d
         * the current node along the path
         */
        def lvls
        lvls = { Integer l, Dependency d ->
            if (!lvl.containsKey(d)) {
                lvl[d] = l
            } else if (l < lvl[d]) {
                lvl[d] = l 
            }
            d.dependsOn.each { dependency ->
                if (!lvl.containsKey(dependency) || l + 1 < lvl[dependency]) {
                    // if we haven't visited or its smaller
                    lvls(l + 1, dependency)
                }
            }
        }
        noDependants(dependencies).each { d ->
            lvls(0, d)
        }
		return lvl
	}

    /** Return a map from Dependency d to it's dependants.
     *
     * @param dependencies
     * the set of all dependencies in a dependency graph
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

    /** Returns a set of all dependencies having no dependants (the "leaves" of the dependency graph). 
     *
     * @param dependencies
     * the set of all dependencies in a dependency graph
     */
    private static Set<Dependency> noDependants(Collection<Dependency> dependencies) {
        Dependency.dependants(dependencies).grep { entry ->
            def (dependency, dependants) = [entry.key, entry.value]
            dependants.size() == 0
        }.collect { it.key }
    }
	
	/** Starting at a node, recurse through its dependants
	 *  Using Depth First Search to Assign vertical Numbers.
	 *  Assigns all the nodes the same Group as the starting node.
	 *  If there is a tie, the nodes are sorted Alphabetically.
	 */
	static numberNodes(dependency,group,depedantsMap,verNumMap,verGrpMap){
		def i=0
		def number
		number={dep,grp,dM,vnM,vgM->
			vnM[dep]=i
			vgM[dep]=grp
			i+=1
			for (d in dM[dep].sort{it.target}){
				number(d,grp,dM,vnM,vgM)
			}
		}
		number(dependency,group,depedantsMap,verNumMap,verGrpMap)
	}
	
	/** Takes in the Set of dependencies of the dependency Graph
	 * For each column in the Dependency Graph it finds the nodes that do not depend
	 * on any other nodes in the column and runs numberNodes on it.
	 * Then it sorts the nodes by Group and then vertical number.
	 * Creates a map from the dependency to its row level
	 * Each of these maps are added to theMap which is returned once the last column's map is added
	 */
	static Map<Dependency, Integer> rowLvls(Set<Dependency> depSet){
		if (depSet ==  ([] as Set)){
			return [:]
		} else {
			/** Getting column Level and dependants Maps
			 */
			def colLvls = Dependency.levels(depSet)
			def dependants = Dependency.dependants(depSet)
			
			def theMap=[:]
			
			/** Iterating through Each column Level
			 */
			for (int n=0; n < colLvls.values().max()+1;n++){
				def levelList =[]
				
				/** creating a list with dependencies that have column Level == n 
				 */
				for (i in depSet){
					if (colLvls[i]==n){
						levelList+=i
					}
				}
				def nulldepOnList=[]
				
				/** Isolating dependants to only within the column Level 
				 * and finding the dependencies that do not 
				 * depend on anything within the column level
				 */
				for (i in levelList){
					dependants[i]=dependants[i].findAll{it in levelList}
					if (i.dependsOn.findAll{it in levelList}==[]){
						nulldepOnList+=i
					}
				}
				
				nulldepOnList.sort{it.target}
				
				def verticalNum=[:]
				def verticalGroup=[:]
				
				/** Creating Maps that 
				 * map dependency to their vertical Number and with
				 * vertical group they fall under 
				 */
				for (i in levelList){
					if (i in nulldepOnList){
						verticalNum[i]=0
						verticalGroup[i]=nulldepOnList.indexOf(i)
					}else{
						verticalNum[i]=null
						verticalGroup[i]=null
					}
				}
				
				/** Calling numberNodes on each dependency in nulldepOnList
				 */
				for (i in nulldepOnList){
					Dependency.numberNodes(i,verticalGroup[i],dependants,verticalNum,verticalGroup)
				}
				
				def numOfGroups = verticalGroup.values().max()+1
				
				def groupList=[]
				/** Sort the dependencies into their groups
				 */
				for (int i=0; i< numOfGroups;i++){
					groupList+=[levelList.findAll{verticalGroup[it]==i}]
				}
				/**Sort each group by vertical Num 
				 * and create a list of all the columns nodes
				 */
				def rowLevelList=[]
				for (i in groupList){
					i.sort{verticalNum[it]}
					rowLevelList+=i
				}
				
				/** Change the list to a map 
				 */
				def rowLevelMap=[:]
				for (i in rowLevelList){
					rowLevelMap[i]=rowLevelList.indexOf(i)
				}
				theMap+=rowLevelMap
			}
			return theMap
		}
	}
}
