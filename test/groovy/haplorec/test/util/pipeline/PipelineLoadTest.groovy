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
public class PipelineLoadTest extends DBTest {
	
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

    /* Load tests.
     */

    void testLoadLotsOfVariants() {
        /* Test loading of variant data in job_patient_variant / _job_patient_variant_gene.
         */
        // number of job_patient_variant records == 2 physical_chromosome * 10 samples * 5000 variantsPerSample
        def variants = generateVariants(5000, 10)
        withSlowQueryLog(sql) {
            shouldRunWithin(seconds: 10) {
                drugRecommendationsTest(variants: variants)
            }
        }
    }

    void buildDependencies(job, stage, built) {
        job[stage].dependsOn.each { d ->
            d.build(built)
        }
    }

   void testGeneHaplotype() {
       /* Test the variantToGeneHaplotype stage of the pipeline.
        */
       def variantsPerHaplotype = 151
       def haplotypesPerGene = 132
       // actual is 10
       def genes = 100
       // number of gene_haplotype_variant records
       def variants = variantsPerHaplotype * haplotypesPerGene * genes
       def sampleData = [
           gene_haplotype_variant: generateGeneHaplotypeVariant(variantsPerHaplotype, haplotypesPerGene, genes),
       ]
       insertSampleData(sampleData)

       // actual is 379
       // only the first 100 (genes) will have haplotypes
       def samples = 379 // genes 
       // actual is 22
       def variantsPerSample = variantsPerHaplotype // variants / samples
       def (_, job) = Pipeline.pipelineJob(sql, variants: generateVariants(variantsPerSample, samples))
       Set<Dependency> built = []
       // buildDependencies(job, 'geneHaplotype', built)
       job.variant.build(built)
       
       withSlowQueryLog(sql) {
           shouldRunWithin(minutes: 5) {
               Pipeline.buildAll(job)
               job.geneHaplotype.build(built)
           }
       }
   }

   def generateGeneHaplotypeVariant(variantsPerHaplotype, haplotypesPerGene, genes) {
       def haplotypes = haplotypesPerGene * genes
       def variants = variantsPerHaplotype * haplotypes
       def rs = 1
       return new Object() {
           def each(Closure f) {
               (1..genes).each { gene ->
                   (1..haplotypesPerGene).each { haplotype ->
                       (1..variantsPerHaplotype).each { variant ->
                           String allele
                           if (variant == 1) {
                               // hack to distinguish haplotype snps; the first snp allele is always the haplotype#
                               allele = haplotype
                           } else {
                               allele = 'A'
                           }
                           f(["g$gene", "*$haplotype", "rs${rs + variant - 1}", allele].collect { it.toString() })
                       }
                   }
                   rs += variantsPerHaplotype
               }
           }
       }
   }

   def generateVariants(variantsPerSample, samples) {
       def variants = variantsPerSample * samples
       def rs = 1
       String zygosity = "hom"
       return new Object() {
           def each(Closure f) {
               (1..samples).each { sample ->
                   (1..variantsPerSample).each { variant ->
                       String allele
                       if (variant == 1) {
                           // hack to distinguish haplotype snps; the first snp allele is always the sample#
                           allele = 1
                       } else {
                           allele = 'A'
                       }
                       ['A', 'B'].each { physical_chromosome ->
                           f(["sample$sample", physical_chromosome, "rs$rs", allele, zygosity].collect { it.toString() })
                       }
                       rs += 1
                   }
               }
           }
       }
   }

}
