package haplorec.test.util

import haplorec.util.Haplotype

import groovy.sql.Sql;
import groovy.util.GroovyTestCase

public class HaplotypeTest extends DBTest {
	
	def TEST_DB = "haplorec_test"
	def TEST_HOST = "localhost"
	def TEST_PORT = 3306
	def TEST_USER = "root"
	def TEST_PASSWORD = ""

    def sql

    void setUp() {
        sql = setUpDB(TEST_DB,
                      host:TEST_HOST,
                      user:TEST_USER,
                      password:TEST_PASSWORD,
                      port:TEST_PORT)
    }

    void tearDown() {
        tearDownDB(TEST_DB, sql)
    }

    void testGenePhenotypeToDrugRecommendation() {
    }

    void testSnpToGeneHaplotype() {
    }

    void testGenotypeToDrugRecommendation() {
    }

}
