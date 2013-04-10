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

    void testGenePhenotypeToDrugRecommendation() {
    }

    void testSnpToGeneHaplotype() {
    }

    void testGenotypeToDrugRecommendation() {
    }
	
	def drugRecommendationsTest(Map kwargs = [:], expectedDrugRecommendationRows) {
		haplorec.util.Sql.createTableFromExisting(sql, 'input_variant',
			existingTable: 'haplotype_snps', 
			columns: ['snp_id', 'allele'], 
			indexColumns: ['snp_id', 'allele'])
		try {
			Haplotype.drugRecommendations(kwargs, sql)
			assertEquals(expectedDrugRecommendationRows, select(sql, 'input_drug_recommendation', ['drug_recommendation_id']))
		} finally {
			sql.execute "drop table if exists input_variant"
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
