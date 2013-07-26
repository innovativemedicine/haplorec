package haplorec.test.util.pipeline

import haplorec.util.pipeline.Algorithm
import haplorec.test.util.pipeline.ReportTest
import haplorec.util.data.GeneHaplotypeMatrix

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

    void testDisambiguateHetsEmpty() {
        /* Trivial case of an empty list of input variants.
         */
        disambiguateHetsTest(
            [],
            [
                AKnownBKnown: [
                ],
                AKnownBNovel: [
                ],
            ],
        )
    }

    void testDisambiguateHets1Het() {
        /* Test that in the special case of 1 heterzygote SNP rs1050828 CT, we get
         * chromosome A:
         * rs1050828 C
         * chromosome B:
         * rs1050828 T
         */
        disambiguateHetsTest(
            generatePatientVariants(
                ['patient1'],
                [
                    het: [
                        rs1050828: 'CT',
                    ],
                ],
            ),
            generateExpected(
                ['rs1050828'],
                [
                    AKnownBKnown: [
                        [
                            ['C'],
                            ['T'],
                        ],
                    ],
                    AKnownBNovel: [
                    ],
                ]
            ),
        )
    }

    void testDisambiguateHets3Snps2KnownHaplotypes() {
        /* Test that we can disambiguate 'Mediterranean Haplotype' / 'B (wildtype)'.
         * Given:
         * rs1050828 CT
         * rs1050829 TC
         * rs5030868 AG
         * The total possible combinations are:
           rs1050828 | rs1050829 | rs5030868 | Haplotype
           C         | T         | A         | 'Mediterranean Haplotype'
           T         | C         | G         | 'A-202A_376G'

           C         | T         | G         | 'B (wildtype)'
           T         | C         | A         | Novel

           C         | C         | A         | Novel
           T         | T         | G         | Novel

           C         | C         | G         | 'A- 680T_376G' OR 'A-968C_376G'
           T         | T         | A         | Novel

         * We don't report Novel/Novel. 
         * Since CCG / TTA could have two possible known haplotypes, we don't call it.
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
            generateExpected(
                ['rs1050828', 'rs1050829', 'rs5030868'],
                [
                    AKnownBKnown: [
                        [
                            ['C', 'T', 'A'],
                            ['T', 'C', 'G'],
                        ],
                    ],
                    AKnownBNovel: [
                        [
                            ['C', 'T', 'G'],
                            ['T', 'C', 'A'],
                        ],
                    ],
                ]
            ),
        )
    }

    void testDisambiguateHets1Novel1KnownHaplotype() {
        /* Test that TC (A-202A_376G) / CG (Novel) is detected (i.e. 1 known, 1 novel).
         */
        disambiguateHetsTest(
            generatePatientVariants(
                ['patient1'],
                [
                    het: [
                        rs1050828: 'TC',
                        rs1050829: 'CG',
                    ],
                ],
            ),
            generateExpected(
                ['rs1050828', 'rs1050829'],
                [
                    AKnownBKnown: [
                    ],
                    AKnownBNovel: [
                        [
                            ['T', 'C'],
                            ['C', 'G'],
                        ],
                    ],
                ]
            ),
        )
    }

    void testDisambiguateHets2PotentialHaplotypeCombos() {
        /* Test that we detect both *1/*2 and *3/*4 (since it's ambiguous which one it might be).
         */
        def geneName = 'g1'
        def snpIds = ['rs1', 'rs2']
        def haplotypeVariants = ReportTest.generateHaplotypeVariants(
            snpIds,
            [
            '*1': ['A', 'A'], 
            '*2': ['T', 'T'], 
            '*3': ['A', 'T'], 
            '*4': ['T', 'A'], 
            ],
        )
        def matrix = new GeneHaplotypeMatrix(geneName: geneName, snpIds: snpIds, haplotypeVariants: haplotypeVariants)
        disambiguateHetsTest(
            matrix: matrix,
            generatePatientVariants(
                ['patient1'],
                [
                    het: [
                        rs1: 'AT',
                        rs2: 'AT',
                    ],
                ],
            ),
            generateExpected(
                ['rs1', 'rs2'],
                [
                    AKnownBKnown: [
                        [
                            ['A', 'A'],
                            ['T', 'T'],
                        ],
                        [
                            ['A', 'T'],
                            ['T', 'A'],
                        ],
                    ],
                    AKnownBNovel: [
                    ],
                ]
            ),
        )
    }

    def generateExpected(snpIds, expectedAlleles) {
        def asRows = { sequencePairs ->
            sequencePairs.collect { s1, s2 ->
                def rows = { s, physicalChromosome ->
                    [s, snpIds].transpose().collect { allele, snpId ->
                        [allele: allele, snp_id: snpId, physical_chromosome: physicalChromosome]
                    }
                }
                (rows(s1, 'A') + rows(s2, 'B')).flatten()
            }
        }
        return [
            AKnownBKnown: asRows(expectedAlleles.AKnownBKnown),
            AKnownBNovel: asRows(expectedAlleles.AKnownBNovel),
        ]
    }

    def disambiguateHetsTest(Map kwargs = [:], hetVariants, expectedRows) {
        if (kwargs.matrix == null) { kwargs.matrix = matrix }
        assertEquals(
            expectedRows,
            Algorithm.disambiguateHets(kwargs.matrix, hetVariants))
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

}
