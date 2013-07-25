package haplorec.util

/* A module for iterating over rows, where a row is a list of of maps, where the maps have a subset 
 * of the first row's keys.  
 * e.g.
 * rows = [
 *     [a:1, b:2, c:3], // this is the header row
 *     [a:1, b:3],
 *     [:]
 * ]
 * The only function that doesn't assume the first row is a header row is noDuplicates.
 */
class Row {

    /* Return an iterator that removes duplicate groups of columns from rows in iter.  Groups are 
     * identified by the groups parameter.
     * e.g.
     * groups = [
     * 
     *     A: [['a'], ['a', 'b']],
     *     // 'a' is the duplicate key (i.e. the primary key for the group), so we only keep columns 
     *     // 'a' and 'b' for the row if the 'a' is unique.
     *
     *     B: [['b'], ['b', 'c']],
     *     // Same idea as above, but notice 'b' is part of this group as well. This means that if 
     *     // we have a row with duplicate 'a' but unique 'b', we still keep 'b' in the row.
     *
     * ]
     */
    static def noDuplicates(Map kwargs = [:], iter, groups) {
        return new Object() {
            def each(Closure f) {
                Map seen = groups.keySet().inject([:]) { m, g -> m[g] = [] as Set; m }
                iter.each { map ->
                    def row = [:]
                    groups.each { g, groupColumns ->
                        def (duplicateKey, columnsToShow) = groupColumns
                        def k = duplicateKey.collect { map[it] }
                        if (!seen[g].contains(k)) {
                            // add the group g
                            seen[g].add(k)
                            columnsToShow.each { row[it] = map[it] }
                        }
                    }
                    f(row)
                }
            }
        }
    }

    /* Assumes iter is ordered by its groups.
     */
    static def groupBy(iter, groups) {
        return new Object() {
            def each(Closure f) {
                def currentGroup = null
                def group = []
                iter.each { map ->
                    if (currentGroup == null) {
                        currentGroup = groups.collect { map[it] }
                        group = [map]
                    } else {
                        def nextGroup = groups.collect { map[it] }
                        if (currentGroup == nextGroup) {
                            group.add(map)
                        } else {
                            f(group)
                            currentGroup = nextGroup
                            group = [map]
                        }
                    }
                }
                if (group != []) {
                    f(group)
                }
            }
        }
    }

    /* Return an iterator that collapses consecutive rows into a single row.  Collapsing of two rows 
     * defaults to occuring only when the two rows have no columns in common.  Note that collapsing 
     * is an accumulative operation and not a pairwise operation; that is:
     * collapse([
     *   [a:1],
     *   [b:1],
     *   [c:1],
     * ])
     * ==
     * [[a:1, b:1, c:1]]
     *
     * Keyword arguments:
     *
     * canCollapse: 
     * a closure of the type { header, accumulated row, current row -> boolean } that returns true 
     * if accumulated row should have all rows in current row added to it. (default: see above)
     */
    static def collapse(Map kwargs = [:], iter) {
        if (kwargs.canCollapse == null) {
            kwargs.canCollapse = { header, lastRow, currentRow ->
                /* By default, we can collapse two rows if:
                 * 1. their columns do not overlap
                 */
                def intersect = new HashSet(lastRow.keySet())
                intersect.retainAll(currentRow.keySet())
                intersect.size() == 0
            }
        }
        if (kwargs.collapse == null) {
            kwargs.collapse = { header, lastRow, currentRow ->
                lastRow.putAll(currentRow)
            }
        }
        return new Object() {
            def each(Closure f) {
                /* Pseudocode:
                 * lastRow = [:]
                 * for each row r:
                 *   try merging r into lastRow 
                 *     if doing so would overwrite a value in lastRow:
                 *       f(lastRow)
                 *       lastRow = [:]
                 * if lastRow != [:]:
                 *   f(lastRow)
                 * def lastRow = null
                 */
                def lastRow = null
                def header
                iter.each { row -> 
                    if (lastRow == null) {
                        // hack to grab the first row
                        lastRow = row
                        header = lastRow.keySet()
                    } else {
                        // try merging r into lastRow 
                        if (kwargs.canCollapse(header, lastRow, row)) {
                            // the two rows have no columns that overlap, we can merge them
                            def r = kwargs.collapse(header, lastRow, row)
                            if (r instanceof Map) {
                                // collapse generated the new last row (instead of just modifying lastRow)
                                lastRow = r
                            }
                        } else {
                            // the two rows have columns that overlap; handle the lastRow
                            f(lastRow)
                            lastRow = row 
                        }
                    }
                }
                if (lastRow != null) {
                    f(lastRow)
                }
            }
        }
    }

    /* Return an iterator that fills missing columns with a value.  
     *
     * Keyword arguments:
     *
     * with: 
     * Either a value to fill all missing columns with, or a closure of type { row, column -> value }
     * that returns the value to store at row[column]
     */
    static def fill(Map kwargs = [:], iter) {
        def with
        if (kwargs.with == null) {
            // kwargs.with = null is the default
            with = { row, column -> null }
        } else if (kwargs.with instanceof Closure) {
            with = kwargs.with
        } else {
            with = { row, column -> kwargs.with }
        }
        def replace
        if (kwargs.replace == null) {
            replace = { row, column ->
                !row.containsKey(column)
            }
        } else {
            replace = kwargs.replace
        }
        return new Object() {
            def each(Closure f) {
                def header
                def i = 0
                iter.each { row ->
                    if (i == 0) {
                        header = row.keySet()
                    }
                    header.each { c ->
                        if (replace(row, c)) {
                            row[c] = with(row, c)
                        }
                    }
                    f(row)
                    i += 1
                }
            }
        }
    }

    /* Given a row iterator, writes a delimiter separated string to the stream, with rows terminated 
     * by newlines.
     * 
     * Keyword arguments:
     *
     * separator: field delimiter (default: '\t')
     */
    static def asDSV(Map kwargs = [:], iter, Appendable stream) {
        if (kwargs.separator == null) { kwargs.separator = '\t' }
        if (kwargs.header == null) {
            kwargs.header = { firstRow ->
                if (firstRow instanceof Map) {
                    firstRow.keySet()
                } else {
                    return firstRow
                }
            }
        }
        if (kwargs.null == null) { 
            kwargs.null = { v -> 
                (v == null) ? '' : v.toString()
            }
        }

        def allButLast = { Map kw = [:], xs ->
            def i = 0
            def previous
            xs.each { x ->
                if (i != 0) {
                    kw.do(previous)
                }
                previous = x
                i += 1
            }
            if (i != 0) {
                // there is a last
                kw.last(previous)
            }
        }

        def header
        def i = 0
        def output = { r ->
            allButLast(r,
            do: {
                stream.append(kwargs.null(it))
                stream.append(kwargs.separator)
            },
            last: { 
                stream.append(kwargs.null(it))
                stream.append(System.getProperty("line.separator"))
            })
        }
        iter.each { row ->
            if (i == 0) {
                header = kwargs['header'](row)
                if (header != null && row instanceof Map) {
                    // The first row is also a data row
                    output(header)
                }
            }
			if (header != null && row instanceof Map) {
				output(header.collect { row[it] })
			} else {
				output(row)
			}
            i += 1
        }
    }

    static def filter(Map kwargs = [:], iter) {
        return new Object() {
            def each(Closure f) {
                iter.each { row ->
                    Map r = kwargs.keep.inject([:]) { m, k ->
                        if (row.containsKey(k)) {
                            m[k] = row[k]
                        }
                        m
                    }
                    f(r)
                }
            }
        }
    }

    static def changeKeys(Map kwargs = [:], iter) {
        if (kwargs.to == null) {
            kwargs.to = { header, h -> h }
        }
        return eachRow(iter) { header, row, _ ->
            row.keySet().inject([:]) { m, k ->
                m[kwargs.to(header, k)] = row[k]
                m
            }
        }
    }

    static def eachRow(iter, Closure f) {
        return new Object() {
            def each(Closure g) {
                def i = 0
                def header
                iter.each { row ->
                    if (i == 0) {
                        header = row.keySet()
                    }
                    g(f(header, row, i))
                    i += 1
                }
            }
        }
    }

}
