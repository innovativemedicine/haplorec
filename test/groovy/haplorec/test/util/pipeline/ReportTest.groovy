package haplorec.test.util.pipeline

import haplorec.util.pipeline.Report
import haplorec.util.pipeline.Report.GeneHaplotypeMatrix.NovelHaplotype;

class ReportTest extends GroovyTestCase {

    def snpIds
    def geneName
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
    }

    def geneHaplotypeMatrixTest(geneName, snpIds, patientVariants, expectedRows) {
        assertEquals(
            expectedRows,
            rows(new Report.GeneHaplotypeMatrix(geneName: geneName, snpIds: snpIds, patientVariants: patientVariants)))
    }

    void testGeneHaplotypeMatrixOnePatient() {
        geneHaplotypeMatrixTest(
            geneName,
            snpIds,
            [
                [patient_id: 'patient1', snp_id: 'rs1050828', allele: 'T', physical_chromosome: 'A'],
                [patient_id: 'patient1', snp_id: 'rs1050829', allele: 'T', physical_chromosome: 'A'],
                [patient_id: 'patient1', snp_id: 'rs5030868', allele: 'G', physical_chromosome: 'A'],

                [patient_id: 'patient1', snp_id: 'rs1050828', allele: 'T', physical_chromosome: 'B'],
                [patient_id: 'patient1', snp_id: 'rs1050829', allele: 'T', physical_chromosome: 'B'],
                [patient_id: 'patient1', snp_id: 'rs5030868', allele: 'G', physical_chromosome: 'B'],
            ],
            [
                [new NovelHaplotype(patientId: 'patient1', physicalChromosome: 'A'), ['T', 'T', 'G', null, null, null]],
                [new NovelHaplotype(patientId: 'patient1', physicalChromosome: 'B'), ['T', 'T', 'G', null, null, null]],
            ])
    }

    void testGeneHaplotypeMatrixTwoPatients() {
        geneHaplotypeMatrixTest(
            geneName,
            snpIds,
            [
                [patient_id: 'patient1', snp_id: 'rs1050828', allele: 'T', physical_chromosome: 'A'],
                [patient_id: 'patient1', snp_id: 'rs1050829', allele: 'T', physical_chromosome: 'A'],
                [patient_id: 'patient1', snp_id: 'rs5030868', allele: 'G', physical_chromosome: 'A'],

                [patient_id: 'patient1', snp_id: 'rs1050828', allele: 'T', physical_chromosome: 'B'],
                [patient_id: 'patient1', snp_id: 'rs1050829', allele: 'T', physical_chromosome: 'B'],
                [patient_id: 'patient1', snp_id: 'rs5030868', allele: 'G', physical_chromosome: 'B'],

                [patient_id: 'patient2', snp_id: 'rs1050828', allele: 'T', physical_chromosome: 'A'],
                [patient_id: 'patient2', snp_id: 'rs1050829', allele: 'T', physical_chromosome: 'A'],
                [patient_id: 'patient2', snp_id: 'rs5030868', allele: 'G', physical_chromosome: 'A'],

                [patient_id: 'patient2', snp_id: 'rs1050828', allele: 'T', physical_chromosome: 'B'],
                [patient_id: 'patient2', snp_id: 'rs1050829', allele: 'T', physical_chromosome: 'B'],
                [patient_id: 'patient2', snp_id: 'rs5030868', allele: 'G', physical_chromosome: 'B'],
            ],
            [
                [new NovelHaplotype(patientId: 'patient1', physicalChromosome: 'A'), ['T', 'T', 'G', null, null, null]],
                [new NovelHaplotype(patientId: 'patient1', physicalChromosome: 'B'), ['T', 'T', 'G', null, null, null]],
                [new NovelHaplotype(patientId: 'patient2', physicalChromosome: 'A'), ['T', 'T', 'G', null, null, null]],
                [new NovelHaplotype(patientId: 'patient2', physicalChromosome: 'B'), ['T', 'T', 'G', null, null, null]],
            ])
    }

    def rows(iter) {
        def xs = []
        iter.each { Object... args -> xs.add(args as List) }
        xs
    }

}
