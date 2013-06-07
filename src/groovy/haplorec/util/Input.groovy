package haplorec.util

import groovy.transform.InheritConstructors;

class Input {
	
	@InheritConstructors
	static class InvalidInputException extends RuntimeException {}
	
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
    /* Keyword arguments:
     *
     * skipHeader:
     * skip the first line of the input, treating it as a header line (default: false)
     * 
     * requireHeader:
     * make sure header matches the first line of the input, otherwise throw InvalidInputException 
     * (default: true if header argument provided, false otherwise)
     * 
     * header:
     * the header line for the file (which may or may not be at the top of the input, see requireHeader)
     * 
     * asList:
     * when true, for the returned iterator, execute .each with the input line as the arguments. When 
     * false, pass the input line as a list instead (default: false)
     * 
     * closeAfter:
     * close the input after finished reading (default: true)
     * 
     * separator:
     * regex pattern to split lines of input by (default: /\t/)
     */
    
    static def dsv(Map kwargs = [:], fileOrStream) {
		if (kwargs.skipHeader == null) { kwargs.skipHeader = false }
        // if they provide a header, then by default we will require it in the input
		if (kwargs.requireHeader == null) { kwargs.requireHeader = (kwargs.header != null) }
        if (kwargs.requireHeader) {
            if (kwargs.header == null) {
                throw new IllegalArgumentException("a header to look for must be provided")
            }
        }
		if (kwargs.asList == null) { kwargs.asList = false }
		if (kwargs.closeAfter == null) { kwargs.closeAfter = true }
		if (kwargs.separator == null) { kwargs.separator = /\t/ }

        def fieldNumbers
        if (kwargs.header == null && kwargs.fieldIndices == null && kwargs.fields == null) {
            fieldNumbers = null
            // skip
        } else if (kwargs.fields != null && kwargs.fields[0] instanceof Integer) {
            fieldNumbers = kwargs.fields
        } else if (kwargs.header != null && kwargs.fieldIndices == null && kwargs.fields == null) {
			fieldNumbers = (1..kwargs.header.size())
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
		return new Object() {
			def each(Closure f) {
                def input = fileOrOpen(fileOrStream)
                try {
                    def iter = input.iterator()
                    int lineNo = 0
                    def _next = { ->
                        lineNo += 1
                        CharSequence l = iter.next()
                        return l.split(kwargs.separator) as List
                    }
                    while (iter.hasNext()) {
                        def fields = _next()

                        /* Handle the header line.
                         */
                        if (lineNo == 1) {
                            if (kwargs.header != null) {
                                if (fields == kwargs.header) {
                                    fields = _next()
                                } else if (kwargs.requireHeader) {
                                    throw new InvalidInputException("Expected header line ${kwargs.header}, at line $lineNo, but saw: ${fields}")
                                } else if (kwargs.skipHeader) {
                                    fields = _next()
                                }
                            } else if (kwargs.skipHeader) {
                                fields = _next()
                            }
                        }

                        /* Extract the fields we want.
                         */
                        def fs 
                        if (fieldNumbers == null) {
                            fs = fields
                        } else {
                            try {
                                fs = fieldNumbers.collect { i -> fields.get(i - 1) }
                            } catch (IndexOutOfBoundsException e) {
                                if (kwargs.header != null) {
                                    throw new InvalidInputException("Expected ${fieldNumbers.max()} columns matching header ${kwargs.header}, but saw ${fields.size()} columns at line $lineNo: $fields", e)
                                } else {
                                    throw new InvalidInputException("Expected ${fieldNumbers.max()} columns, but saw ${fields.size()} columns at line $lineNo: $fields", e)
                                }
                            }
                        }

                        /* Execute the provided closure on our fields.
                         */
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
