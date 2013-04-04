package haplorec.test.util

import haplorec.util.Haplotype

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
        setKwargsDefaults(kwargs)
        // default keyword arguments 
        def sql = sqlInstance(kwargs)
        // groovy.sql.Sql is doing some miserably weird garbage behind the scenes when calling 
        // Sql.execute(GString sql); in particular the error message suggests it's replacing 
        // ${SOMEVAR} with 'blah' (single quotes _included_), hence our explicit toString call.
		sql.execute "drop database if exists ${db}".toString()
		sql.execute "create database ${db}".toString()
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

}
