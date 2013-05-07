package haplorec.test.util

import haplorec.util.pipeline.Pipeline;

import groovy.sql.Sql;
import groovy.util.GroovyTestCase

public class DBTest extends GroovyTestCase {
	
    private def setKwargsDefaults(Map kwargs) {
        if (kwargs.host == null) { kwargs.host = 'localhost' }
        if (kwargs.user == null) { kwargs.user = 'root' }
        if (kwargs.password == null) { kwargs.password = '' }
        if (kwargs.port == null) { kwargs.port = 3306 }
    }

    def setUpDB(Map kwargs = [:], db) {
		// default keyword arguments
        setKwargsDefaults(kwargs)
		if (kwargs.schemaFile != null) { kwargs.schema = new File(kwargs.schemaFile).text }
        def sql = sqlInstance(kwargs)
        // groovy.sql.Sql is doing some miserably weird garbage behind the scenes when calling 
        // Sql.execute(GString sql); in particular the error message suggests it's replacing 
        // ${SOMEVAR} with 'blah' (single quotes _included_), hence our explicit toString call.
		sql.execute "drop database if exists ${db}".toString()
		sql.execute "create database ${db}".toString()
		if (kwargs.schema != null) {
			// def sqlDB = sqlInstance(kwargs, db)
			sql.execute "use ${db}".toString()
			kwargs.schema.replaceAll(/\s*--.*/, "")
						 .replaceAll(/\n\s*\n/, "")
						 .tokenize(';')
						 .grep { !(it =~ /^\s*$/) }
						 .each { stmt -> 
				sql.execute stmt 
			}
		}
        sql.close()
        return sqlInstance(kwargs, db) 
    }

    def tearDownDB(db, Sql sql) {
        sql.execute "drop database ${db}".toString()
        sql.close()
    }

    def sqlInstance(Map kwargs = [:], db = null) {
        setKwargsDefaults(kwargs)
        def url = "jdbc:mysql://${kwargs.host}:${kwargs.port}"
        def sql = Sql.newInstance((db == null) ? url : "${url}/${db}", kwargs.user, kwargs.password)
        assert sql != null
        return sql
    }

    def insert(sql, table, columns, rows) {
        if (rows.size() > 0) {
            sql.withBatch("insert into ${table}(${columns.join(', ')}) values (${(['?'] * columns.size()).join(', ')})".toString()) { ps ->
                rows.each { r -> ps.addBatch(r) }
            }
        }
    }

    def select(sql, table, columns) {
        def hashRowsToListRows = { rows, cols -> 
            rows.collect { r -> 
                cols.collect { r[it] } 
            } 
        }
        return hashRowsToListRows(sql.rows("select ${columns.join(', ')} from $table".toString()), columns)
    }
	
	def selectCount(groovy.sql.Sql sql, table) {
		return (
			sql.rows("select count(*) as count from $table".toString())
		)[0]['count']
	}

    private static def parseCreateTableStatement(createTableStatment) {
        def m = (createTableStatment =~ /(?i)create\s+table\s+([^(]+)/)
        def (_, tableName) = m[0]
        def columns = (createTableStatment.substring(m.end()) =~ /\s*(?:(\w+)\s+\w+(?:\([^)]*\))?\s*,?\s*)/).collect { match -> match[1] }
        return [tableName, columns]
    }

    /* [(createTableStatment, [dataRow]) or (createTableStatment)] table
     */
    def tableTest(sql, tables, Closure doTest) {
        def tableNames = []
        try {
            // create tables and insert data
            tables.each { t ->
                if (t.size() == 2) {
                    def (createTableStatment, rows) = t
                    def (tableName, columns) = parseCreateTableStatement(createTableStatment)
                    sql.execute createTableStatment
                    tableNames.add(tableName)
                    insert(sql, tableName, columns, rows)
                } else {
                    def (createTableStatment) = t
                    def (tableName, _) = parseCreateTableStatement(createTableStatment)
                    tableNames.add(tableName)
                    sql.execute createTableStatment
                }
            }
            // run the test
            doTest()
        } finally {
            tableNames.each { 
                sql.execute "drop table if exists $it".toString()
            }
        }
    }

}
