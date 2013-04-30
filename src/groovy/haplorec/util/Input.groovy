package haplorec.util

class Input {
	
	class InvalidInputException extends RuntimeException {}
	
	static private def fileOrOpen(file) {
		if (file != null) {
			if (file instanceof CharSequence) {
				return new FileReader(file)
			} else {
				return file
			}
		} else {
			throw new IllegalArgumentException("Expected either an input stream or a filepath but saw ${file}")
		}
	}
	
	private static def idxMap(Map kwargs = [:], List xs) {
		int i = 0
		if (kwargs.start != null) { i = kwargs.start }
		return xs.inject([:]) { m, x ->
			m[x] = i
			i += 1
            m
		}
	}

	/*
    dsv(file)
    dsv(file, fields:[1,2,3])
    dsv(file, fields:['genotype_id', 'snp_id'], header:['some_other_stuff', 'genotype_id', 'snp_id'])
    dsv(file, fields:['genotype_id', 'snp_id'], fieldIndices:[genotype_id:2, snp_id:5])
    */
    static def dsv(Map kwargs = [:], fileOrStream) {
		// Whether there's a header at the top of the file
		if (kwargs.skipHeader == null) { kwargs.skipHeader = true }
		if (kwargs.asList == null) { kwargs.asList = false }
		if (kwargs.closeAfter == null) { kwargs.closeAfter = true }
		if (kwargs.separator == null) { kwargs.separator = /\t/ }

        def fieldNumbers
        if (kwargs.header == null && kwargs.fieldIndices == null && kwargs.fields == null) {
            fieldNumbers = null
            // skip
        } else if (kwargs.fields[0] instanceof Integer) {
            fieldNumbers = kwargs.fields
        } else {
            def collectFieldIndices = { fieldIndices ->
                kwargs.fields.collect { field ->
                    field = fieldIndices[field] 
                    if (field == null) {
                        throw new IllegalArgumentException("no such field $field in fieldIndices: $fieldIndices")
                    }
                    return field
                }
            }
            if (kwargs.header == null && kwargs.fieldIndices == null) {
                throw new IllegalArgumentException("field names were provided for reading $fileOrStream but one of header or fieldIndices arguments are needed")
            } else if (kwargs.fieldIndices != null) {
                fieldNumbers = collectFieldIndices(kwargs.fieldIndices)
            } else {
                fieldNumbers = collectFieldIndices(idxMap(kwargs.header, start:1))
            }
        }
        if (kwargs.fieldIndices == null) { kwargs.fieldIndices = idxMap(kwargs.fields) }
		return new Object() {
			def each(Closure f) {
                def input = fileOrOpen(fileOrStream)
                try {
                    def iter = input.iterator()
                    int lineNo = 0
                    def _next = { ->
                        lineNo += 1
                        CharSequence l = iter.next()
                        return l.split(kwargs.separator)
                    }
                    while (iter.hasNext()) {
                        def fields = _next()
                        if (lineNo == 1) {
                            if (kwargs.header != null && fields != kwargs.header) {
                                throw new InvalidInputException("Expected header line ${kwargs.header}, but saw ${fields}")
                            }
                            if (kwargs.skipHeader) {
                                fields = _next()
                            }
                        }
                        def fs = (fieldNumbers != null) ?
                            fieldNumbers.collect { i -> fields[i - 1] } :
                            fields
                        if (kwargs.asList) {
                            f(fs)
                        } else {
                            f(*fs)
                        }
                    }
                } finally {
                    if (kwargs.closeAfter) {
                        input.close()
                    }
                }
			}
		}
    }

    static def applyF(boolean asList, List xs, Closure f) {
        if (asList) {
            f(xs)
        } else {
            f(*xs)
        }
    }

}
