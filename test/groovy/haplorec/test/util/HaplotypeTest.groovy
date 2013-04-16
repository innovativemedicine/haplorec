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
    def columnsToCheck

    void setUp() {
        sql = setUpDB(TEST_DB,
                      host:TEST_HOST,
                      user:TEST_USER,
                      password:TEST_PASSWORD,
                      port:TEST_PORT,
					  schemaFile:TEST_SCHEMA_FILE)

        columnsToCheck = [
            job_patient_drug_recommendation : ['job_id', 'patient_id', 'drug_recommendation_id'],
            job_patient_gene_haplotype      : ['job_id', 'patient_id', 'gene_name', 'haplotype_name'],
            job_patient_genotype            : ['job_id', 'patient_id', 'gene_name', 'haplotype_name1', 'haplotype_name2'],
            job_patient_gene_phenotype      : ['job_id', 'patient_id', 'gene_name', 'phenotype_name'],
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

    def assertJobTable(Map kwargs = [:], jobTable, expectedRows) {
		assertEquals(expectedRows.sort(), select(sql, jobTable, columnsToCheck[jobTable]).sort())
    }

	void testDrugRecommendationsAmbiguous() {
        
        // the same sampleData as testDrugRecommendationsAmbiguous ...
        def sampleData = [
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
                // ... with the addition haplotypes for all possible combinations of (rs1, A/G) and (rs2, A/G)
                ['g1', '*3', 'rs1', 'G'],
                ['g1', '*3', 'rs2', 'G'],
                ['g1', '*4', 'rs1', 'G'],
                ['g1', '*4', 'rs2', 'A'],
                ['g1', '*5', 'rs1', 'A'],
                ['g1', '*5', 'rs2', 'A'],
            ],
            genotype_phenotype: [
                ['g1', '*1', '*1', 'homozygote normal'],
                ['g1', '*1', '*2', 'heterozygote'],
				['g1', '*1', '*3', 'heterozygote'],
                ['g1', '*2', '*2', "nonfunctional"],
            ],
            genotype_drug_recommendation: [
            ],
        ]
		
        /* Tests: 
         *
         * We reject snp->haplotype mappings for a patient (patient3) who has more than 2 
         * heterozygote calls involved in the snp_id's required to resolve haplotypes for a gene 
         * (i.e. an ambiguous association of variations on physical chromosomes).

         * We don't reject snp->haplotype mappings for a patient (patient2) who has 1 heterozygote 
         * call involved in the snp_id's required to resolve haplotypes for a gene (i.e. an 
         * umambiguous association of variations on physical chromosomes).
         */
        drugRecommendationsTest(
            sampleData: sampleData,
			variants: [
                ['patient1', 'chr1A', 'rs1', 'A', 'hom'],
                ['patient1', 'chr1B', 'rs1', 'A', 'hom'],
                ['patient1', 'chr1A', 'rs2', 'G', 'hom'],
                ['patient1', 'chr1B', 'rs2', 'G', 'hom'],

                // patient2 has 1 heterozygote snp_id used in snp->haplotype mappings for g1, so we can still use it
                ['patient2', 'chr1A', 'rs1', 'A', 'het'],
                ['patient2', 'chr1B', 'rs1', 'G', 'het'],
                ['patient2', 'chr1A', 'rs2', 'G', 'hom'],
                ['patient2', 'chr1B', 'rs2', 'G', 'hom'],

                // patient2 has 2 heterozygote snp_id's used in snp->haplotype mappings for g1, so we can't use it
                ['patient3', 'chr1A', 'rs1', 'A', 'het'],
                ['patient3', 'chr1B', 'rs1', 'G', 'het'],
                ['patient3', 'chr1A', 'rs2', 'A', 'het'],
                ['patient3', 'chr1B', 'rs2', 'G', 'het'],
			])
		assertJobTable('job_patient_gene_haplotype', [
			[1, 'patient2', 'g1', '*1'],
			[1, 'patient2', 'g1', '*3'],
            [1, 'patient1', 'g1', '*1'],
			[1, 'patient1', 'g1', '*1'],
		])
		assertJobTable('job_patient_genotype', [
			[1, 'patient2', 'g1', '*1', '*3'],
            [1, 'patient1', 'g1', '*1', '*1'],
		])
		assertJobTable('job_patient_gene_phenotype', [
			[1, 'patient2', 'g1', 'heterozygote'],
			[1, 'patient1', 'g1', 'homozygote normal'],
		])
		assertJobTable('job_patient_drug_recommendation', [
			[1, 'patient2', 2],
			[1, 'patient1', 1],
		])

    }

	void testDrugRecommendationsUnambiguous() {

        def sampleData = [
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
		
        /* A job with a single patient with snps resolve to a *1/*1 genotype, a homozygote normal 
         * phenotype, and a 'drug' recommendation
         */
		drugRecommendationsTest(
			sampleData: sampleData,
			ambiguousVariants: false,
			variants: [
				// TODO: i think select disinct is messing up selectWhereSetContains for this testcase; figure out how to work around that
				['patient1', 'chr1A', 'rs1', 'A'],
				['patient1', 'chr1A', 'rs2', 'G'],
				['patient1', 'chr1B', 'rs1', 'A'],
				['patient1', 'chr1B', 'rs2', 'G'],
			])
		assertJobTable('job_patient_gene_haplotype', [
			[1, 'patient1', 'g1', '*1'],
			[1, 'patient1', 'g1', '*1'],
		])
		assertJobTable('job_patient_genotype', [
			[1, 'patient1', 'g1', '*1', '*1'],
		])
		assertJobTable('job_patient_gene_phenotype', [
			[1, 'patient1', 'g1', 'homozygote normal'],
		])
		assertJobTable('job_patient_drug_recommendation', [
			[1, 'patient1', 1],
		])

        /* A second job that's the same as before; here we're testing to make sure multiple jobs are 
         * independent (the results of the first job are still present), and that we can handle 
         * multiple patients.
         */
        drugRecommendationsTest(
			ambiguousVariants: false,
			variants: [
				['patient2', 'chr1A', 'rs1', 'A'],
				['patient2', 'chr1A', 'rs2', 'G'],
				['patient2', 'chr1B', 'rs1', 'A'],
				['patient2', 'chr1B', 'rs2', 'G'],
                // from last test's job 1
				['patient1', 'chr1A', 'rs1', 'A'],
				['patient1', 'chr1A', 'rs2', 'G'],
				['patient1', 'chr1B', 'rs1', 'A'],
				['patient1', 'chr1B', 'rs2', 'G'],
			])
		assertJobTable('job_patient_gene_haplotype', [
			[2, 'patient2', 'g1', '*1'],
			[2, 'patient2', 'g1', '*1'],
            [2, 'patient1', 'g1', '*1'],
			[2, 'patient1', 'g1', '*1'],
            // from last test's job 1
            [1, 'patient1', 'g1', '*1'],
			[1, 'patient1', 'g1', '*1'],
		])
		assertJobTable('job_patient_genotype', [
			[2, 'patient2', 'g1', '*1', '*1'],
            [2, 'patient1', 'g1', '*1', '*1'],
            // from last test's job 1
            [1, 'patient1', 'g1', '*1', '*1'],
		])
		assertJobTable('job_patient_gene_phenotype', [
			[2, 'patient2', 'g1', 'homozygote normal'],
			[2, 'patient1', 'g1', 'homozygote normal'],
            // from last test's job 1
			[1, 'patient1', 'g1', 'homozygote normal'],
		])
		assertJobTable('job_patient_drug_recommendation', [
			[2, 'patient2', 1],
			[2, 'patient1', 1],
            // from last test's job 1
			[1, 'patient1', 1],
		])

	}

}
