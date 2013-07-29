package haplorec.util.data

import groovy.sql.GroovyRowResult
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.ResultSetMetaData

class Sql {

    private static def iterAsList(iter) {
        def xs = []
        iter.each { xs.add(it) }
        return xs
    }

    private static GroovyRowResult nextRow(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        ArrayList list = new ArrayList(50);
        HashMap row = new HashMap(columns);
        for(int i = 1; i <= columns; i++){           
            row.put(md.getColumnName(i), rs.getObject(i));
        }
        return new GroovyRowResult(row);
    }
	
	private static def withConnection(groovy.sql.Sql sql, Closure f) {
		if (sql.connection == null) {
			// This Sql instance was created from a DataSource.
			def connection = sql.dataSource.getConnection()
			try {
				f(connection)
			} finally {
                // Return the connection to the connection pool.
				connection.close()
			}
		} else {
			f(sql.connection)
		}
	}

    /* Keyword arguments:
     * Boolean cacheRows: if true, .each can be called multiple times; otherwise it can only be called once.
     */
    private static def stmtIter(Map kwargs = [:], connection, String sqlStr) {
        if (kwargs.cacheRows == null) { kwargs.cacheRows = true }
        def stmt = connection.prepareStatement(sqlStr)
        ResultSet rs = null
        List rows
        /* Whether or not we have iterated through all results in the ResultSet.
         */
        boolean scannedResultSet = false
        return new Object() {
            def execute(Object... params) {
                (1..params.size()).each { i ->
                    stmt.setObject(i, params[i-1])
                }
                rs = stmt.executeQuery()
                rows = []
            }
            def each(Closure f) {
                if (scannedResultSet && kwargs.cacheRows) {
                    rows.each(f)
                } else {
                    if (rs == null) {
                        throw new IllegalStateException("Must call execute(params) before next()")
                    }
                    while (rs.next()) {
                        def row = nextRow(rs)
                        if (kwargs.cacheRows) {
                            rows.add(row)
                        }
                        f(row)
                    }
                    rs.close()
                    scannedResultSet = true
                }
            }
        }
    }

}
