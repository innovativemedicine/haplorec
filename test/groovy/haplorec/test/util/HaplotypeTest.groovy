package haplorec.test.util

import java.util.Map;

import haplorec.util.Haplotype

import groovy.sql.Sql
import groovy.util.GroovyTestCase

public class HaplotypeTest extends DBTest {
	
	def TEST_DB = "haplorec_test"
	def TEST_HOST = "localhost"
	def TEST_PORT = 3306
	def TEST_USER = "root"
	def TEST_PASSWORD = ""
	def TEST_SCHEMA_FILE = 'src/sql/mysql/haplorec.sql'

    def sql

    void setUp() {
        sql = setUpDB(TEST_DB,
                      host:TEST_HOST,
                      user:TEST_USER,
                      password:TEST_PASSWORD,
                      port:TEST_PORT,
					  schemaFile:TEST_SCHEMA_FILE)
    }

    void tearDown() {
        tearDownDB(TEST_DB, sql)
    }

//    void testGenePhenotypeToDrugRecommendation() {
//    }
//
//    void testSnpToGeneHaplotype() {
//    }
//
//    void testGenotypeToDrugRecommendation() {
//    }
	
	def drugRecommendationsTest(Map kwargs = [:], expectedDrugRecommendationRows) {
		try {
			Haplotype.drugRecommendations(kwargs, sql)
			assertEquals(expectedDrugRecommendationRows, select(sql, 'job_drug_recommendation', ['drug_recommendation_id']))
		} finally {
			sql.execute "drop table if exists job_variant"
		}
	}
	
	void testDrugRecommendations() {
		drugRecommendationsTest(
			variants: [
			], 
			[
			])
	}

}
