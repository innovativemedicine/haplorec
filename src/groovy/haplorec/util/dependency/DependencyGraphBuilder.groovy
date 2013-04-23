package haplorec.util.dependency

import groovy.util.ObjectGraphBuilder
import groovy.util.ObjectGraphBuilder.ChildPropertySetter;
import groovy.util.ObjectGraphBuilder.IdentifierResolver

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
