package haplorec.test.util.pipeline

import haplorec.util.pipeline.Algorithm
import haplorec.test.util.pipeline.ReportTest
import haplorec.util.pipeline.Report.GeneHaplotypeMatrix

class AlgorithmTest extends GroovyTestCase {

    GeneHaplotypeMatrix matrix 
    void setUp() {
        /* Gene: G6PD
         * Haplotype Name          | rs1050828 | rs1050829 | rs5030868 | rs137852328 | rs76723693 | rs2230037
         * B (wildtype)            | C         | T         | G         | C           | A          | G
         * A-202A_376G             | T         | C         | G         | C           | A          | G
         * A- 680T_376G            | C         | C         | G         | A           | A          | G
         * A-968C_376G             | C         | C         | G         | C           | G          | G
         * Mediterranean Haplotype | C         | T         | A         | C           | A          | A
         */
        def geneName = 'G6PD'
        def snpIds = ['rs1050828', 'rs1050829', 'rs5030868', 'rs137852328', 'rs76723693', 'rs2230037']
        def haplotypeVariants = ReportTest.generateHaplotypeVariants(
            ['rs1050828', 'rs1050829', 'rs5030868', 'rs137852328', 'rs76723693', 'rs2230037'],
            [
            'B (wildtype)':            ['C', 'T', 'G', 'C', 'A', 'G'],
            'A-202A_376G':             ['T', 'C', 'G', 'C', 'A', 'G'],
            'A- 680T_376G':            ['C', 'C', 'G', 'A', 'A', 'G'],
            'A-968C_376G':             ['C', 'C', 'G', 'C', 'G', 'G'],
            'Mediterranean Haplotype': ['C', 'T', 'A', 'C', 'A', 'A'],
            ],
        )
        matrix = new GeneHaplotypeMatrix(geneName: geneName, snpIds: snpIds, haplotypeVariants: haplotypeVariants)
    }

    def disambiguateHetsTest(hetVariants, expectedRows) {
        assertEquals(
            expectedRows,
            Algorithm.disambiguateHets(matrix, hetVariants))
    }

    static def generatePatientVariants(Map kwargs = [:], patientIds, variants) {
        if (kwargs.physicalChromosomes == null) { kwargs.physicalChromosomes = ['A', 'B'] }
        def xs = []
        patientIds.each { patientId ->
            def addVariants = { vars, zygosity ->
                vars.each { snpId, alleleStr ->
                    def alleles = (zygosity == 'het') ? alleleStr as List : [alleleStr]*2 
                    [alleles, kwargs.physicalChromosomes].transpose().each { allele, physicalChromosome ->
                        xs.add([patient_id: patientId, snp_id: snpId, allele: allele, physical_chromosome: physicalChromosome, zygosity: zygosity])
                    }
                }
            }
            if (variants.containsKey('het')) {
                addVariants(variants.het, 'het')
            }
            if (variants.containsKey('hom')) {
                addVariants(variants.hom, 'hom')
            }
        }
        return xs
    }

    void testDisambiguateHets3Snps2KnownHaplotypes() {
        /* Test that we can disambiguate 'Mediterranean Haplotype' / 'B (wildtype)'.
         */
        disambiguateHetsTest(
            generatePatientVariants(
                ['patient1'],
                [
                    het: [
                        rs1050828: 'CT',
                        rs1050829: 'TC',
                        rs5030868: 'AG',
                    ],
                ],
            ),
            [
                [allele: 'C', snp_id: 'rs1050828', physical_chromosome: 'A'],
                [allele: 'T', snp_id: 'rs1050829', physical_chromosome: 'A'],
                [allele: 'A', snp_id: 'rs5030868', physical_chromosome: 'A'],
                [allele: 'T', snp_id: 'rs1050828', physical_chromosome: 'B'],
                [allele: 'C', snp_id: 'rs1050829', physical_chromosome: 'B'],
                [allele: 'G', snp_id: 'rs5030868', physical_chromosome: 'B'],
            ])
    }

}

