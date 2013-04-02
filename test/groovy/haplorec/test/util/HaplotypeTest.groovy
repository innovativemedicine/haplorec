package haplorec.test.util

import groovy.sql.Sql;
import groovy.util.GroovyTestCase

public class HaplotypeTest extends GroovyTestCase {
	
	def TEST_DB = "haplorec_test"
	def TEST_PORT = 3306
	def TEST_URL = "jdbc:mysql://localhost:${TEST_PORT}"
	def TEST_USER = "root"
	def TEST_PASSWORD = ""
	def TEST_DRIVER = "com.mysql.jdbc.Driver"

    Sql sql

    void setUp() {
        sql = Sql.newInstance(TEST_URL, TEST_USER, TEST_PASSWORD)
        // sql = Sql.newInstance(TEST_URL, TEST_USER, TEST_PASSWORD, TEST_DRIVER)
        assert sql != null
        // groovy.sql.Sql is doing some miserably weird garbage behind the scenes when calling 
        // Sql.execute(GString sql); in particular the error message suggests it's replacing 
        // ${SOMEVAR} with 'blah' (single quotes _included_), hence our explicit toString call.
        sql.execute "create database ${TEST_DB}".toString()
        sql.close()
        sql = Sql.newInstance("${TEST_URL}/${TEST_DB}", TEST_USER, TEST_PASSWORD)
    }

    void tearDown() {
        sql.execute "drop database ${TEST_DB}".toString()
        sql.close()
    }

    void testSnpsToHaplotypes() {
    }

    void testGenePhenotypeToDrugRecommendation() {
    }

    void testSnpToGeneHaplotype() {
    }

    void testGenotypeToDrugRecommendation() {
    }

}
