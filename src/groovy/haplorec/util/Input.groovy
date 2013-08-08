package haplorec.util

import groovy.transform.InheritConstructors;

class Input {
	
	@InheritConstructors
	static class InvalidInputException extends RuntimeException {}
	
    /** Return an iterator over a delimiter separated file.
     * Provides options for skipping a header (either by skipping a line it we see it, or just 
     * skipping the first line regardless).
     *
     * @param fileOrStream
     * Either a string representing a filepath which is opened is read from, or an already open 
     * input stream.
     *
     * Keyword arguments:
     *
     * Optional:
     * 
     * @param kwargs.fields
     * a list of field names belonging to kwargs.header, or a list of 1-based indices of fields to 
     * keep
     *
     * @param kwargs.header
     * the header line for the file (which may or may not be at the top of the input, see requireHeader)
     * 
     * @param kwargs.separator
     * regex pattern to split lines of input by (default: /\t/)
     *
     * @param kwargs.skipHeader
     * skip the first line of the input, treating it as a header line (default: false)
     * 
     * @param kwargs.requireHeader
     * make sure header matches the first line of the input, otherwise throw InvalidInputException 
     * (default: true if header argument provided, false otherwise)
     * 
     * @param kwargs.asList
     * when true, for the returned iterator, execute .each with the input line as the arguments. When 
     * false, pass the input line as a list instead (default: false)
     * 
     * @param kwargs.closeAfter
     * close the input stream after finished reading (default: true)
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
        if (kwargs.header == null && kwargs.fields == null) {
            fieldNumbers = null
            // skip
        } else if (kwargs.fields != null && kwargs.fields[0] instanceof Integer) {
            fieldNumbers = kwargs.fields
        } else if (kwargs.header != null && kwargs.fields == null) {
			fieldNumbers = (1..kwargs.header.size())
    	} else {
            Map fieldIndices = idxMap(kwargs.header, start:1)
            fieldNumbers = kwargs.fields.collect { field ->
                field = fieldIndices[field] 
                if (field == null) {
                    throw new IllegalArgumentException("no such field $field in header: $field")
                }
                return field
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

    /* If it's a string, open it as a file, otherwise assume its an open input stream.
     */
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
	
    /* idxMap(['one', 'two', 'three'], start: 1) == [one: 1, two: 2, three: 3]
     */
	private static def idxMap(Map kwargs = [:], List xs) {
		int i = 0
		if (kwargs.start != null) { i = kwargs.start }
		return xs.inject([:]) { m, x ->
			m[x] = i
			i += 1
            m
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
