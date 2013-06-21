package haplorec.test.util.pipeline

import org.junit.Ignore;

import haplorec.test.util.DBTest
import haplorec.test.util.TimedTest

import haplorec.util.Input.InvalidInputException
import haplorec.util.Sql

import haplorec.util.dependency.Dependency

import haplorec.util.pipeline.Pipeline
import haplorec.util.pipeline.PipelineInput
import haplorec.util.pipeline.Report

import groovy.util.GroovyTestCase

@Mixin(TimedTest)
public class PipelineTest extends DBTest {
	
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
            job_patient_phenotype_drug_recommendation : ['job_id', 'patient_id', 'drug_recommendation_id'],
            job_patient_genotype_drug_recommendation  : ['job_id', 'patient_id', 'drug_recommendation_id'],
            job_patient_gene_haplotype                : ['job_id', 'patient_id', 'gene_name', 'haplotype_name'],
            job_patient_genotype                      : ['job_id', 'patient_id', 'gene_name', 'haplotype_name1', 'haplotype_name2'],
            job_patient_gene_phenotype                : ['job_id', 'patient_id', 'gene_name', 'phenotype_name'],
            job_patient_variant                       : ['job_id', 'patient_id', 'physical_chromosome', 'snp_id', 'allele', 'zygosity'],
            job_patient_unique_haplotype              : ['job_id', 'patient_id', 'gene_name', 'physical_chromosome'],
        ]
    }

    void tearDown() {
        tearDownDB(TEST_DB, sql)
    }

	def drugRecommendationsTest(Map kwargs = [:]) {
		Pipeline.drugRecommendations(kwargs, sql)
	}

    def insertSampleData(sampleData) {
        sampleData.each { kv ->
            def (table, data) = [kv.key, kv.value]
            if (data instanceof java.util.List) {
                Sql.insert(sql, table, data)
            } else if (data instanceof Map) {
                Sql.insert(sql, table, data.columns, data.rows)
            } else {
                Sql.insert(sql, table, Sql.tableColumns(sql, table), data)
            }
        }
    }

    def assertJobTable(Map kwargs = [:], jobTable, expectedRows) {
        def expect = expectedRows.sort()
        def got = select(sql, jobTable, columnsToCheck[jobTable]).sort()
		assert expect == got
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
        insertSampleData(sampleData)
		
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
		assertJobTable('job_patient_phenotype_drug_recommendation', [
			[1, 'patient2', 2],
			[1, 'patient1', 1],
		])

    }

    /* Test the pipeline using a real variant input file.  This tests the following behaviours:
     * - reading the variant file format of a well-formatted file
     * TODO:
     * - handling calls without any allele (TODO: are these calls that we couldn't read?)
     * - handling heterzygous calls (without ambiguity)
     */
	void testDrugRecommendationsRealVariants() {

        def sampleData = [
            /* TODO: add some sampleData that actually uses the variants data; probably best to do 
             * this once we actually gather some real sampleData.
             */
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
        insertSampleData(sampleData)

		drugRecommendationsTest(
			variants: "test/in/2_samples.txt")
		
		// TODO: assert stuff, possibly the variants table itself

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
        insertSampleData(sampleData)
		
        /* A job with a single patient with snps resolve to a *1/*1 genotype, a homozygote normal 
         * phenotype, and a 'drug' recommendation
         */
		drugRecommendationsTest(
			variants: [
				// TODO: i think select disinct is messing up selectWhereEitherSetContains for this testcase; figure out how to work around that
				['patient1', 'chr1A', 'rs1', 'A', 'hom'],
				['patient1', 'chr1A', 'rs2', 'G', 'hom'],
				['patient1', 'chr1B', 'rs1', 'A', 'hom'],
				['patient1', 'chr1B', 'rs2', 'G', 'hom'],
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
		assertJobTable('job_patient_phenotype_drug_recommendation', [
			[1, 'patient1', 1],
		])

        /* A second job that's the same as before; here we're testing to make sure multiple jobs are 
         * independent (the results of the first job are still present), and that we can handle 
         * multiple patients.
         */
        drugRecommendationsTest(
			variants: [
				['patient2', 'chr1A', 'rs1', 'A', 'hom'],
				['patient2', 'chr1A', 'rs2', 'G', 'hom'],
				['patient2', 'chr1B', 'rs1', 'A', 'hom'],
				['patient2', 'chr1B', 'rs2', 'G', 'hom'],
                // from last test's job 1
				['patient1', 'chr1A', 'rs1', 'A', 'hom'],
				['patient1', 'chr1A', 'rs2', 'G', 'hom'],
				['patient1', 'chr1B', 'rs1', 'A', 'hom'],
				['patient1', 'chr1B', 'rs2', 'G', 'hom'],
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
		assertJobTable('job_patient_phenotype_drug_recommendation', [
			[2, 'patient2', 1],
			[2, 'patient1', 1],
            // from last test's job 1
			[1, 'patient1', 1],
		])

	}

	void testDrugRecommendationsDuplicateDrugRecommendations() {

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
            ],
            genotype_phenotype: [
                ['g1', '*1', '*1', 'homozygote normal'],
                ['g1', '*1', '*2', 'heterozygote'],
                ['g1', '*2', '*2', "nonfunctional"],
            ],
            genotype_drug_recommendation: [
                ['g1', '*1', '*1', 1],
            ],
        ]
        insertSampleData(sampleData)

        drugRecommendationsTest(
            variants: [
                ['patient1', 'chr1A', 'rs1', 'A', 'hom'],
                ['patient1', 'chr1B', 'rs1', 'A', 'hom'],
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
        /* This mapping will be found twice; once from job_patient_genotype (through 
         * genotype_drug_recommendation) and once from job_patient_gene_phenotype (through 
         * gene_phenotype_drug_recommendation).
         */
        assertJobTable('job_patient_genotype_drug_recommendation', [
            [1, 'patient1', 1],
        ])
        assertJobTable('job_patient_phenotype_drug_recommendation', [
            [1, 'patient1', 1],
        ])



    }

    private def rowsAsStream(Map kwargs = [:], rows) {
        if (kwargs.separator == null) { kwargs.separator = '\t' }
        StringBuffer buffer = new StringBuffer()
        rows.each { row ->
            buffer.append row.join(kwargs.separator)
            buffer.append System.getProperty("line.separator")
        }
        return new BufferedReader(new StringReader(buffer.toString()))
    }
	
	void testDrugRecommendationsInputGenotypes() {
		
		drugRecommendationsTest(
			genotypes: [rowsAsStream([
				PipelineInput.inputHeaders.genotype,
				['patient1', 'g1', '*1', '*1'],
			])])
		assertJobTable('job_patient_genotype', [
			[1, 'patient1', 'g1', '*1', '*1'],
		])
		
	}
	
	void testDrugRecommendationsInputGenotypesWithoutHeader() {
		
		drugRecommendationsTest(
			genotypes: [rowsAsStream([
				// without header
				['patient1', 'g1', '*1', '*1'],
			])])
		assertJobTable('job_patient_genotype', [
			[1, 'patient1', 'g1', '*1', '*1'],
		])
		
	}
	
	void testDrugRecommendationsInvalidInputGenotypes() {
		def msg
		
		// too few columns
		msg = shouldFail(InvalidInputException) {
			drugRecommendationsTest(
				genotypes: [rowsAsStream([
					['patient1', 'g1', '*1'],
				])])
		}
		assert msg =~ /Expected \d+ columns matching header/
		
	}

	void testDrugRecommendationsInputVariantsWithoutHeader() {
		
		drugRecommendationsTest(
			variants: [rowsAsStream([
				['RA_CCP_RXN1_TCC-P5-7+SickKidsP12_May2011', '1', '1', 'N02', 'chr1_117098850', 'C', 'A.Conservative', '1063-117507', 'Automatic'],
			])])
        assertJobTable('job_patient_variant', [
            [1, '1063-117507', 'A', 'chr1_117098850', 'C', 'hom'],
            [1, '1063-117507', 'B', 'chr1_117098850', 'C', 'hom'],
        ])
		
	}
	
	void testDrugRecommendationsInvalidInputVariants() {

		def msg
		
        msg = shouldFail(InvalidInputException) {
            drugRecommendationsTest(
                variants: [rowsAsStream([
                    PipelineInput.inputHeaders.variant,
                    ['RA_CCP_RXN1_TCC-P5-7+SickKidsP12_May2011', '1', '1', 'N02', 'chr1_117098850', 'CA', 'A.Conservative', '1063-117507', 'Automatic'],
                    ['RA_CCP_RXN1_TCC-P5-7+SickKidsP12_May2011', '1', '1', 'N02', 'chr1_196991682', 'G', 'A.Conservative', '1063-117507', 'Automatic'],
                    ['RA_CCP_RXN1_TCC-P5-7+SickKidsP12_May2011', '1', '1', 'N02', 'chr22_35868467', 'CAT', 'A.Conservative', '1063-117507', 'Automatic'],
                ])]
            )
        }
        assert msg =~ /Number of alleles was/

        // too few columns
        msg = shouldFail(InvalidInputException) {
            drugRecommendationsTest(
                variants: [rowsAsStream([
                    PipelineInput.inputHeaders.variant,
                    ['RA_CCP_RXN1_TCC-P5-7+SickKidsP12_May2011', '1', '1', 'N02', 'chr1_117098850', 'CA', 'A.Conservative', '1063-117507', 'Automatic'],
                    // truncated # of columns
                    ['RA_CCP_RXN1_TCC-P5-7+SickKidsP12_May2011', '1'],
                    ['RA_CCP_RXN1_TCC-P5-7+SickKidsP12_May2011', '1', '1', 'N02', 'chr22_35868467', 'CA', 'A.Conservative', '1063-117507', 'Automatic'],
                ])]
            )
        }
        assert msg =~ /Expected \d+ columns matching header/

    }
	
	void testGeneHaplotypeStrictSubsetUnambiguous() {
		
        /* Test that haplotypes where we have a strict subset of the variants needed to call it, and it isn't ambiguous, are accepted.
         */
        def sampleData = [
            gene_haplotype_variant: [
                ['g1', '*1', 'rs1', 'A'],
                ['g1', '*1', 'rs2', 'G'],
            ],
        ]
        insertSampleData(sampleData)

        drugRecommendationsTest(
            variants: [
                ['patient1', 'chr1A', 'rs1', 'A', 'hom'],
                ['patient1', 'chr1B', 'rs1', 'A', 'hom'],
            ])
        assertJobTable('job_patient_gene_haplotype', [
            [1, 'patient1', 'g1', '*1'],
			[1, 'patient1', 'g1', '*1'],
        ])

	}

	void testGeneHaplotypeStrictSubsetUnambiguousPlusExtra() {
		
        /* Test that haplotypes where we have a strict subset of the variants needed to call it, and it isn't ambiguous, and we have some extra unrelated snp, 
         * are accepted.
         */
        def sampleData = [
            gene_haplotype_variant: [
                ['g1', '*1', 'rs1', 'A'],
                ['g1', '*1', 'rs2', 'G'],
            ],
        ]
        insertSampleData(sampleData)

        drugRecommendationsTest(
            variants: [
                ['patient1', 'chr1A', 'rs1', 'A', 'hom'],
                ['patient1', 'chr1B', 'rs1', 'A', 'hom'],
                ['patient1', 'chr1A', 'rs3', 'A', 'hom'],
                ['patient1', 'chr1B', 'rs3', 'A', 'hom'],
            ])
        assertJobTable('job_patient_gene_haplotype', [
            [1, 'patient1', 'g1', '*1'],
			[1, 'patient1', 'g1', '*1'],
        ])

	}

    void testUniqueHaplotypes() {
        /* Test that haplotypes where we have a strict subset of snp_id's, but some unqiue alleles are ignored.
         * TODO: ideally we should report unique haplotypes, probably by adding a new node in the graph.
         */
        def sampleData = [
            gene_haplotype_variant: [
                ['g1', '*1', 'rs1', 'A'],
                ['g1', '*1', 'rs2', 'G'],
            ],
        ]
        insertSampleData(sampleData)

        drugRecommendationsTest(
            variants: [
                ['patient1', 'chr1A', 'rs1', 'A', 'hom'],
                ['patient1', 'chr1B', 'rs1', 'A', 'hom'],
                ['patient1', 'chr1A', 'rs2', 'T', 'hom'],
                ['patient1', 'chr1B', 'rs2', 'T', 'hom'],
            ])
        assertJobTable('job_patient_gene_haplotype', [
            // should be empty
        ])
    }

	void testGeneHaplotypeStrictSubsetAmbiguous() {
		
        /* Test that haplotypes where we have a strict subset of the variants needed to call it, but it's ambiguous, are rejected.
         */
        def sampleData = [
            gene_haplotype_variant: [
                ['g1', '*1', 'rs1', 'A'],
                ['g1', '*1', 'rs2', 'G'],
                ['g1', '*2', 'rs1', 'A'],
                ['g1', '*2', 'rs2', 'T'],
            ],
        ]
        insertSampleData(sampleData)

        drugRecommendationsTest(
            variants: [
                ['patient1', 'chr1A', 'rs1', 'A', 'hom'],
                ['patient1', 'chr1B', 'rs1', 'A', 'hom'],
            ])
        assertJobTable('job_patient_gene_haplotype', [
            // should be empty
        ])

	}


	void testGenotypeOnlySubset() {
		
        /* Test that drug recommendations where we only have some (i.e. a strict subset) of the genotypes needed to call it are ignored.
         */
        def sampleData = [
            drug_recommendation: [
                columns:['id', 'recommendation'],
                rows:[
                    [1, 'drug'],
                    [2, 'some drug'],
                    [3, 'no drug'],
                ],
            ],
            genotype_drug_recommendation: [
                ['g1', '*1', '*1', 1],
                ['g2', '*1', '*2', 1],
                ['g3', '*3', '*4', 1],
                ['g4', '*1', '*1', 1],
            ],
        ]
        insertSampleData(sampleData)

        drugRecommendationsTest(
            genotypes: [
                ['patient1', 'g1', '*1', '*1'],
                ['patient1', 'g2', '*1', '*2'],
                ['patient1', 'g3', '*3', '*4'],
                // missing g4 *1/*1 needed for drug_recommendation 1
            ])
        assertJobTable('job_patient_genotype_drug_recommendation', [
            // should be empty
        ])

	}

	void testGenotypeSuperset() {
		
        /* Test that drug recommendations where we have all and more (i.e. a superset) of the genotypes needed to call it aren't ignored.
         */
        def sampleData = [
            drug_recommendation: [
                columns:['id', 'recommendation'],
                rows:[
                    [1, 'drug'],
                    [2, 'some drug'],
                    [3, 'no drug'],
                ],
            ],
            genotype_drug_recommendation: [
                ['g1', '*1', '*1', 1],
                ['g2', '*1', '*2', 1],
                ['g3', '*3', '*4', 1],
                ['g4', '*1', '*1', 1],
            ],
        ]
        insertSampleData(sampleData)

        drugRecommendationsTest(
            genotypes: [
                ['patient1', 'g1', '*1', '*1'],
                ['patient1', 'g2', '*1', '*2'],
                ['patient1', 'g3', '*3', '*4'],
                ['patient1', 'g4', '*1', '*1'],
                ['patient1', 'g5', '*1', '*1'],
                // missing g4 *1/*1 needed for drug_recommendation 1
            ])
        assertJobTable('job_patient_genotype_drug_recommendation', [
            [1, 'patient1', 1],
        ])

	}

	void testGenePhenotypeOnlySubset() {
		
        /* Test that phenotypes where we only have some (i.e. a strict subset) of the genotypes needed to call it are ignored.
         */
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
                ['g2', 'homozygote', 1],
                ['g3', 'heterozygote', 1],
            ],
        ]
        insertSampleData(sampleData)

        drugRecommendationsTest(
            genePhenotypes: [
                ['patient1', 'g1', 'homozygote normal'],
                ['patient1', 'g2', 'homozygote'],
                // missing g3 heterozygote needed for drug_recommendation 1
            ])
        assertJobTable('job_patient_phenotype_drug_recommendation', [
            // should be empty
        ])

	}

	void testGenePhenotypeSuperset() {
		
        /* Test that phenotypes where we have all and more (i.e. a superset) of the genotypes needed to call it aren't ignored.
         */
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
                ['g2', 'homozygote', 1],
                ['g3', 'heterozygote', 1],
            ],
        ]
        insertSampleData(sampleData)

        drugRecommendationsTest(
            genePhenotypes: [
                ['patient1', 'g1', 'homozygote normal'],
                ['patient1', 'g2', 'homozygote'],
                ['patient1', 'g3', 'heterozygote'],
            ])
        assertJobTable('job_patient_phenotype_drug_recommendation', [
            [1, 'patient1', 1],
        ])

	}

    void testUniqueHaplotypeExistingVariants() {
        /* Test for a unique haplotype consisting of existing variants (from already known 
         * haplotypes).
         */
        def sampleData = [
            gene_haplotype_variant: [
                ['g1', '*1', 'rs1', 'A'],
                ['g1', '*1', 'rs2', 'G'],
                ['g1', '*1', 'rs3', 'C'],
                ['g1', '*2', 'rs1', 'T'],
                ['g1', '*2', 'rs2', 'C'],
                ['g1', '*2', 'rs3', 'C'],
            ],
        ]
        insertSampleData(sampleData)

		drugRecommendationsTest(
			variants: [
                ['patient1', 'chr1A', 'rs1', 'T', 'hom'],
                ['patient1', 'chr1B', 'rs1', 'T', 'hom'],
                ['patient1', 'chr1A', 'rs2', 'G', 'hom'],
                ['patient1', 'chr1B', 'rs2', 'G', 'hom'],
                ['patient1', 'chr1A', 'rs3', 'C', 'hom'],
                ['patient1', 'chr1B', 'rs3', 'C', 'hom'],
            ])
        assertJobTable('job_patient_gene_haplotype', [])
        assertJobTable('job_patient_unique_haplotype', [
            [1, 'patient1', 'g1', 'chr1A'],
            [1, 'patient1', 'g1', 'chr1B'],
        ])
    }
	
	void testUniqueHaplotypeNovelVariant() {
		/* Test for a unique haplotype consisting of a novel variant (does not exist in any known
		 * haplotypes).
		 */
		def sampleData = [
			gene_haplotype_variant: [
				['g1', '*1', 'rs1', 'A'],
				['g1', '*1', 'rs2', 'G'],
				['g1', '*1', 'rs3', 'C'],
				['g1', '*2', 'rs1', 'T'],
				['g1', '*2', 'rs2', 'C'],
				['g1', '*2', 'rs3', 'C'],
			],
		]
		insertSampleData(sampleData)

		drugRecommendationsTest(
			variants: [
				['patient1', 'chr1A', 'rs1', 'T', 'hom'],
				['patient1', 'chr1B', 'rs1', 'T', 'hom'],
				['patient1', 'chr1A', 'rs2', 'C', 'hom'],
				['patient1', 'chr1B', 'rs2', 'C', 'hom'],
				['patient1', 'chr1A', 'rs3', 'T', 'hom'],
				['patient1', 'chr1B', 'rs3', 'T', 'hom'],
			])
		assertJobTable('job_patient_gene_haplotype', [])
		assertJobTable('job_patient_unique_haplotype', [
			[1, 'patient1', 'g1', 'chr1A'],
			[1, 'patient1', 'g1', 'chr1B'],
		])
	}
	
	void testUniqueHaplotypeNovelVariantIncomplete() {
		/* Test for a unique haplotype consisting of a novel variant (does not exist in any known
		 * haplotypes), that is lacking variants for all snps in a gene.
		 */
		def sampleData = [
			gene_haplotype_variant: [
				['g1', '*1', 'rs1', 'A'],
				['g1', '*1', 'rs2', 'G'],
				['g1', '*1', 'rs3', 'C'],
				['g1', '*2', 'rs1', 'T'],
				['g1', '*2', 'rs2', 'C'],
				['g1', '*2', 'rs3', 'C'],
			],
		]
		insertSampleData(sampleData)

		drugRecommendationsTest(
			variants: [
				['patient1', 'chr1A', 'rs1', 'G', 'hom'],
				['patient1', 'chr1B', 'rs1', 'G', 'hom'],
			])
		assertJobTable('job_patient_gene_haplotype', [])
		assertJobTable('job_patient_unique_haplotype', [
			[1, 'patient1', 'g1', 'chr1A'],
			[1, 'patient1', 'g1', 'chr1B'],
		])
	}
	
	void testUniqueHaplotypeExistingVariantsIncomplete() {
		/* Test for a unique haplotype consisting of existing variants (from already known 
         * haplotypes), that is lacking variants for all snps in a gene.
		 */
		def sampleData = [
			gene_haplotype_variant: [
				['g1', '*1', 'rs1', 'A'],
				['g1', '*1', 'rs2', 'G'],
				['g1', '*1', 'rs3', 'C'],
				['g1', '*2', 'rs1', 'T'],
				['g1', '*2', 'rs2', 'C'],
				['g1', '*2', 'rs3', 'C'],
			],
		]
		insertSampleData(sampleData)

		drugRecommendationsTest(
			variants: [
                ['patient1', 'chr1A', 'rs1', 'T', 'hom'],
                ['patient1', 'chr1B', 'rs1', 'T', 'hom'],
                ['patient1', 'chr1A', 'rs2', 'G', 'hom'],
                ['patient1', 'chr1B', 'rs2', 'G', 'hom'],
			])
		assertJobTable('job_patient_gene_haplotype', [])
		assertJobTable('job_patient_unique_haplotype', [
			[1, 'patient1', 'g1', 'chr1A'],
			[1, 'patient1', 'g1', 'chr1B'],
		])
	}
	
	void testNoUniqueHaplotypeAmbiguousExisting() {
		/* Test for the absence of any unique haplotypes, since the variants observed are an ambiguous subset of existing haplotypes.
		 */
		def sampleData = [
			gene_haplotype_variant: [
				['g1', '*1', 'rs1', 'A'],
				['g1', '*1', 'rs2', 'G'],
				['g1', '*1', 'rs3', 'C'],
				['g1', '*2', 'rs1', 'T'],
				['g1', '*2', 'rs2', 'C'],
				['g1', '*2', 'rs3', 'C'],
			],
		]
		insertSampleData(sampleData)

		drugRecommendationsTest(
			variants: [
				['patient1', 'chr1A', 'rs3', 'C', 'hom'],
				['patient1', 'chr1B', 'rs3', 'C', 'hom'],
			])
		assertJobTable('job_patient_gene_haplotype', [])
		assertJobTable('job_patient_unique_haplotype', [])
	}

}
