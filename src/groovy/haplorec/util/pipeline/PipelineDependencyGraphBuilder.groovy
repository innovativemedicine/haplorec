package haplorec.util.pipeline

import haplorec.util.dependency.DependencyGraphBuilder;

/** Same as DependencyGraphBuilder, but for haplorec.util.pipeline.Dependency.
 * We need this class in the same namespace (haplorec.util.pipeline) as 
 * haplorec.util.pipeline.Dependency so that the dependency(...) call in the DSL references the 
 * right class.
 */
class PipelineDependencyGraphBuilder extends DependencyGraphBuilder {
	public PipelineDependencyGraphBuilder() {
		super()
		this.classNameResolver = "haplorec.util.pipeline"
	}
}
