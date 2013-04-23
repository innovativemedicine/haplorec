package haplorec.util.haplotype

import haplorec.util.dependency.DependencyGraphBuilder;

class HaplotypeDependencyGraphBuilder extends DependencyGraphBuilder {
	public HaplotypeDependencyGraphBuilder() {
		super()
//			this.classNameResolver = "haplorec.util.Haplotype.Dependency"
//			this.classNameResolver = haplorec.util.Haplotype.Dependency
//			this.newInstanceResolver = { klass, attributes ->
//				Dependency.newInstance(attributes)
//			}
		this.classNameResolver = "haplorec.util.haplotype"
	}
}