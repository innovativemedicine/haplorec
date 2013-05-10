package haplorec.util.pipeline

import haplorec.util.dependency.DependencyGraphBuilder;

class PipelineDependencyGraphBuilder extends DependencyGraphBuilder {
	public PipelineDependencyGraphBuilder() {
		super()
//			this.classNameResolver = "haplorec.util.Pipeline.Dependency"
//			this.classNameResolver = haplorec.util.Pipeline.Dependency
//			this.newInstanceResolver = { klass, attributes ->
//				Dependency.newInstance(attributes)
//			}
		this.classNameResolver = "haplorec.util.pipeline"
	}
}
