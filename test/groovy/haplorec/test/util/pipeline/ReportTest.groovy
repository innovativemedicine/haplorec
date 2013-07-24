package haplorec.test.util.pipeline

import haplorec.util.pipeline.Report
import haplorec.util.data.GeneHaplotypeMatrix
import haplorec.util.data.GeneHaplotypeMatrix.NovelHaplotype;
import haplorec.util.data.GeneHaplotypeMatrix.Haplotype;

class ReportTest extends GroovyTestCase {

    def snpIds
    def geneName
    def haplotypeVariants
    void setUp() {
        /* Gene: G6PD
         * Haplotype Name          | rs1050828 | rs1050829 | rs5030868 | rs137852328 | rs76723693 | rs2230037
         * B (wildtype)            | C         | T         | G         | C           | A          | G
         * A-202A_376G             | T         | C         | G         | C           | A          | G
         * A- 680T_376G            | C         | C         | G         | A           | A          | G
         * A-968C_376G             | C         | C         | G         | C           | G          | G
         * Mediterranean Haplotype | C         | T         | A         | C           | A          | A
         */
        geneName = 'G6PD'
        snpIds = ['rs1050828', 'rs1050829', 'rs5030868', 'rs137852328', 'rs76723693', 'rs2230037']
        haplotypeVariants = generateHaplotypeVariants(
            ['rs1050828', 'rs1050829', 'rs5030868', 'rs137852328', 'rs76723693', 'rs2230037'],
            [
            'B (wildtype)':            ['C', 'T', 'G', 'C', 'A', 'G'],
            'A-202A_376G':             ['T', 'C', 'G', 'C', 'A', 'G'],
            'A- 680T_376G':            ['C', 'C', 'G', 'A', 'A', 'G'],
            'A-968C_376G':             ['C', 'C', 'G', 'C', 'G', 'G'],
            'Mediterranean Haplotype': ['C', 'T', 'A', 'C', 'A', 'A'],
            ],
        )
    }

    def geneHaplotypeMatrixTest(geneName, snpIds, patientVariants, haplotypeVariants, expectedRows) {
        assertEquals(
            expectedRows,
            rows(new GeneHaplotypeMatrix(geneName: geneName, snpIds: snpIds, patientVariants: patientVariants, haplotypeVariants: haplotypeVariants)))
    }

    static def generatePatientVariants(Map kwargs = [:], patientIds, variants) {
        if (kwargs.physicalChromosomes == null) { kwargs.physicalChromosomes = ['A', 'B'] }
        def xs = []
        patientIds.each { patientId ->
            kwargs.physicalChromosomes.each { physicalChromosome ->
                def addVariants = { vars, zygosity ->
                    vars.each { snpId, allele ->
                        xs.add([patient_id: patientId, snp_id: snpId, allele: allele, physical_chromosome: physicalChromosome, zygosity: zygosity])
                    }
                }
                if (variants.containsKey('het')) {
                    addVariants(variants.het, 'het')
                }
                if (variants.containsKey('hom')) {
                    addVariants(variants.hom, 'hom')
                }
                if (!variants.containsKey('het') && !variants.containsKey('hom')) {
                    addVariants(variants, null)
                }
            }
        }
        return xs
    }

    static def zipEach(iters, Closure f) {
        int i = 0
        while (true) {
            if (iters.any { it.size() == i }) {
                // We reached the end of one of the iterables.
                return
            }
            f(*iters.collect { iter -> iter[i] })
            i += 1
        }
    }

    static def generateHaplotypeVariants(Map kwargs = [:], snpIds, haplotypes) {
        def xs = []
        haplotypes.each { haplotypeName, alleles ->
            zipEach([snpIds, alleles]) { snpId, allele ->
                xs.add([haplotype_name: haplotypeName, snp_id: snpId, allele: allele])
            }
        }
        return xs
    }

    void testGeneHaplotypeMatrixOnePatient() {
        geneHaplotypeMatrixTest(
            geneName,
            snpIds,
            generatePatientVariants(
                ['patient1'],
                [
                    rs1050828: 'T',
                    rs1050829: 'T',
                    rs5030868: 'G',
                ]
            ),
            null,
            [
                [new NovelHaplotype(patientId: 'patient1', physicalChromosome: 'A'), ['T', 'T', 'G', null, null, null]],
                [new NovelHaplotype(patientId: 'patient1', physicalChromosome: 'B'), ['T', 'T', 'G', null, null, null]],
            ])
    }

    void testGeneHaplotypeMatrixTwoPatients() {
        geneHaplotypeMatrixTest(
            geneName,
            snpIds,
            generatePatientVariants(
                ['patient1', 'patient2'],
                [
                    rs1050828: 'T',
                    rs1050829: 'T',
                    rs5030868: 'G',
                ]
            ),
            null,
            [
                [new NovelHaplotype(patientId: 'patient1', physicalChromosome: 'A'), ['T', 'T', 'G', null, null, null]],
                [new NovelHaplotype(patientId: 'patient1', physicalChromosome: 'B'), ['T', 'T', 'G', null, null, null]],
                [new NovelHaplotype(patientId: 'patient2', physicalChromosome: 'A'), ['T', 'T', 'G', null, null, null]],
                [new NovelHaplotype(patientId: 'patient2', physicalChromosome: 'B'), ['T', 'T', 'G', null, null, null]],
            ])
    }

    void testGeneHaplotypeMatrixOnePatientWithHaplotypeVariants() {
        geneHaplotypeMatrixTest(
            geneName,
            snpIds,
            generatePatientVariants(
                ['patient1'],
                [
                    rs1050828: 'T',
                    rs1050829: 'T',
                    rs5030868: 'G',
                ]
            ),
            haplotypeVariants,
            [
                [new Haplotype(haplotypeName: 'B (wildtype)'),            ['C', 'T', 'G', 'C', 'A', 'G']],
                [new Haplotype(haplotypeName: 'A-202A_376G'),             ['T', 'C', 'G', 'C', 'A', 'G']],
                [new Haplotype(haplotypeName: 'A- 680T_376G'),            ['C', 'C', 'G', 'A', 'A', 'G']],
                [new Haplotype(haplotypeName: 'A-968C_376G'),             ['C', 'C', 'G', 'C', 'G', 'G']],
                [new Haplotype(haplotypeName: 'Mediterranean Haplotype'), ['C', 'T', 'A', 'C', 'A', 'A']],

                [new NovelHaplotype(patientId: 'patient1', physicalChromosome: 'A'), ['T', 'T', 'G', null, null, null]],
                [new NovelHaplotype(patientId: 'patient1', physicalChromosome: 'B'), ['T', 'T', 'G', null, null, null]],
            ])
    }

    void testGeneHaplotypeMatrixTwoPatientsWithHaplotypeVariants() {
        geneHaplotypeMatrixTest(
            geneName,
            snpIds,
            generatePatientVariants(
                ['patient1', 'patient2'],
                [
                    rs1050828: 'T',
                    rs1050829: 'T',
                    rs5030868: 'G',
                ]
            ),
            haplotypeVariants,
            [
                [new Haplotype(haplotypeName: 'B (wildtype)'),            ['C', 'T', 'G', 'C', 'A', 'G']],
                [new Haplotype(haplotypeName: 'A-202A_376G'),             ['T', 'C', 'G', 'C', 'A', 'G']],
                [new Haplotype(haplotypeName: 'A- 680T_376G'),            ['C', 'C', 'G', 'A', 'A', 'G']],
                [new Haplotype(haplotypeName: 'A-968C_376G'),             ['C', 'C', 'G', 'C', 'G', 'G']],
                [new Haplotype(haplotypeName: 'Mediterranean Haplotype'), ['C', 'T', 'A', 'C', 'A', 'A']],

                [new NovelHaplotype(patientId: 'patient1', physicalChromosome: 'A'), ['T', 'T', 'G', null, null, null]],
                [new NovelHaplotype(patientId: 'patient1', physicalChromosome: 'B'), ['T', 'T', 'G', null, null, null]],

                [new NovelHaplotype(patientId: 'patient2', physicalChromosome: 'A'), ['T', 'T', 'G', null, null, null]],
                [new NovelHaplotype(patientId: 'patient2', physicalChromosome: 'B'), ['T', 'T', 'G', null, null, null]],
            ])
    }

    static def rows(iter) {
        def xs = []
        iter.each { Object... args -> xs.add(args as List) }
        xs
    }

}
