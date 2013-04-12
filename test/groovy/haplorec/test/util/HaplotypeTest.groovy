package haplorec.test.util

import java.util.Map;

import haplorec.util.Haplotype
import haplorec.util.Sql

import groovy.util.GroovyTestCase

public class HaplotypeTest extends DBTest {
	
	def TEST_DB = "haplorec_test"
	def TEST_HOST = "localhost"
	def TEST_PORT = 3306
	def TEST_USER = "root"
	def TEST_PASSWORD = ""
	def TEST_SCHEMA_FILE = 'src/sql/mysql/haplorec.sql'

    def sql
    def sampleData
    def columnsToCheck

    void setUp() {
        sql = setUpDB(TEST_DB,
                      host:TEST_HOST,
                      user:TEST_USER,
                      password:TEST_PASSWORD,
                      port:TEST_PORT,
					  schemaFile:TEST_SCHEMA_FILE)
        sampleData = [
            drug_recommendation: [
                columns:['id', 'recommendation'],
                rows:[
                    [1, 'drug'],
                    [2, 'some drug'],
                    [3, 'no drug'],
                ],
            ],
            gene_phenotype_drug_recommendation: [
                ['g1', 'homozygote normal', 1],
                ['g1', 'heterozygote', 2],
                ['g1', "nonfunctional", 3],
            ],
            gene_haplotype_variant: [
                ['g1', '*1', 'rs1', 'A'],
                ['g1', '*1', 'rs2', 'G'],
                ['g1', '*2', 'rs3', 'C'],
                ['g1', '*2', 'rs4', 'T'],
            ],
            genotype_phenotype: [
                ['g1', '*1', '*1', 'homozygote normal'],
                ['g1', '*1', '*2', 'heterozygote'],
                ['g1', '*2', '*2', "nonfunctional"],
            ],
            genotype_drug_recommendation: [
            ],
        ]
        columnsToCheck = [
            job_drug_recommendation : ['drug_recommendation_id'],
            job_gene_haplotype      : ['gene_name', 'haplotype_name'],
            job_genotype            : ['gene_name', 'haplotype_name1', 'haplotype_name2'],
            job_gene_phenotype      : ['gene_name', 'phenotype_name'],
        ]
    }

    void tearDown() {
        tearDownDB(TEST_DB, sql)
    }

	def drugRecommendationsTest(Map kwargs = [:]) {
        def sampleData = kwargs.remove('sampleData')
        insertSampleData(kwargs, sampleData)
		Haplotype.drugRecommendations(kwargs, sql)
	}

    def insertSampleData(Map kwargs = [:], sampleData) {
        sampleData.each { kv ->
            def (table, data) = [kv.key, kv.value]
            if (data instanceof java.util.List) {
                Sql.insert(sql, table, data)
            } else {
                Sql.insert(sql, table, data.columns, data.rows)
            }
        }
    }

    def assertJobTable(jobTable, expectedRows) {
		assertEquals(expectedRows, select(sql, jobTable, columnsToCheck[jobTable]))
    }

	void testDrugRecommendations() {
//		drugRecommendationsTest(variants: [], [])

        // gene_haplotype_variant contains variants
        // gene_haplotype_variant equals variants
        // variants contains variants

        // homozygote normal
		// TODO: see ask lars question regarding duplicate variants
//		drugRecommendationsTest(
//			sampleData: sampleData,
//			variants: [
//				['rs1', 'A'],
//				['rs2', 'G'],
//				['rs1', 'A'],
//				['rs2', 'G'],
//			])
//        assertJobTable('job_gene_haplotype', [
//            ['g1', '*1'],
//            ['g1', '*1'],
//        ])
//        assertJobTable('job_genotype', [
//            ['g1', '*1', '*1'],
//        ])
//        assertJobTable('job_gene_phenotype', [
//            ['g1', 'drug'],
//        ])
//        assertJobTable('job_drug_recommendation', [
//            [1],
//        ])
		
		drugRecommendationsTest(
			sampleData: sampleData,
			variants: [
				['rs1', 'A'],
				['rs2', 'G'],
			])
		assertJobTable('job_gene_haplotype', [
			['g1', '*1'],
		])
		assertJobTable('job_genotype', [
			['g1', '*1', '*1'],
		])
		assertJobTable('job_gene_phenotype', [
			['g1', 'drug'],
		])
		assertJobTable('job_drug_recommendation', [
			[1],
		])
        /*
        // heterozygote
		drugRecommendationsTest(
			sampleData: sampleData,
			variants: [
				['rs1', 'A'],
				['rs2', 'G'],
				['rs3', 'C'],
				['rs4', 'T'],
			], 
			[
                [2],
			])

        // nonfunctional 
		drugRecommendationsTest(
			sampleData: sampleData,
			variants: [
				['rs3', 'C'],
				['rs4', 'T'],
				['rs3', 'C'],
				['rs4', 'T'],
			], 
			[
                [3],
			])

        // none (only 1 normal haplotype)
		drugRecommendationsTest(
			sampleData: sampleData,
			variants: [
				['rs1', 'A'],
				['rs2', 'G'],
			], 
			[
			])

        // none (only 1 nonfunctional haplotype)
		drugRecommendationsTest(
			sampleData: sampleData,
			variants: [
				['rs3', 'C'],
				['rs4', 'T'],
			], 
			[
			])

        // none (no haplotypes)
		drugRecommendationsTest(
			sampleData: sampleData,
			variants: [
				['rs1', 'A'],
				['rs3', 'C'],
			], 
			[
			])

*/
	}

}
