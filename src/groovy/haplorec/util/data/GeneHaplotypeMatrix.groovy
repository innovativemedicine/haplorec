package haplorec.util.data

import haplorec.util.Row
import haplorec.util.pipeline.Pipeline

import java.sql.Connection
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/** A GeneHaplotypeMatrix represents a gene-haplotype matrix as seen on PharmGKB,
 *  in addition to (though not necessarily) patient variants for novel haplotypes (identified from a 
 *  particular job).
 *
 * For example:
 * - from the haplotypes tab of http://www.pharmgkb.org/gene/PA28469
 *
 * Gene: G6PD
 * Haplotype Name          | rs1050828 | rs1050829 | rs5030868 | rs137852328 | rs76723693 | rs2230037
 * B (wildtype)            | C         | T         | G         | C           | A          | G
 * A-202A_376G             | T         | C         | G         | C           | A          | G
 * A- 680T_376G            | C         | C         | G         | A           | A          | G
 * A-968C_376G             | C         | C         | G         | C           | G          | G
 * Mediterranean Haplotype | C         | T         | A         | C           | A          | A
 * Sample 1, chrA (1/1)    | C         | T         | A         | C           | A          | A         // novel patient haplotype
 *
 * Rows of the GeneHaplotypeMatrix can be iterated over using 
 * geneHaplotypeMatrix.each { (Haplotype or NovelHaplotype) haplotype, List alleles ->
 *     ...
 * }
 *
 * A GeneHaplotypeMatrix can be used to resolve which haplotypes a input list of variants (belonging 
 * to the same physical chromosome) might represent (see variantsToHaplotypes).
 *
 * Instances should be obtained using novelHaplotypeMatrix or haplotypeMatrix.
 */
class GeneHaplotypeMatrix {

    /** Return a GeneHaplotypeMatrix annotated with patient variants for novel haplotypes, 
     * identified in a particular job.
     * @param jobId
     * the job from which to obtain patientVariants for this GeneHaplotypeMatrix
     * @param geneName
     * which gene this matrix is for
     */
    static GeneHaplotypeMatrix novelHaplotypeMatrix(Map kwargs = [:], sql, jobId, geneName) {
        kwargs += Pipeline.tables()
        def patientVariants = sql.rows("""\
                |select snp_id, allele, patient_id, het_combo, het_combos, physical_chromosome
                |from ${kwargs.novelHaplotype}
                |join ${kwargs.variant} using (job_id, patient_id, physical_chromosome)
                |where job_id = :job_id and gene_name = :gene_name
                |order by job_id, gene_name, patient_id, physical_chromosome, het_combo, snp_id
                |""".stripMargin(),
                [job_id: jobId, gene_name: geneName])
        return geneHaplotypeMatrix(kwargs, sql, geneName, patientVariants)
    }
    
    /** Return a GeneHaplotypeMatrix (without any patient variants).
     * @param geneName
     * which gene this matrix is for
     */
    static GeneHaplotypeMatrix haplotypeMatrix(Map kwargs = [:], sql, geneName) {
        return geneHaplotypeMatrix(kwargs, sql, geneName, null)
    }

    /** Return a GeneHaplotypeMatrix annotated with the given patientVariants.
     * @param geneName
     * which gene this matrix is for
     */
    private static GeneHaplotypeMatrix geneHaplotypeMatrix(Map kwargs = [:], sql, geneName, patientVariants) {
        def haplotypeVariants = sql.rows("""
                |select haplotype_name, snp_id, allele
                |from gene_haplotype_variant
                |where gene_name = :gene_name
                |order by gene_name, haplotype_name, snp_id
                |""".stripMargin(),
                [gene_name: geneName])
        return new GeneHaplotypeMatrix(
            geneName: geneName,
            snpIds: sql.rows("""
                |select snp_id 
                |from gene_snp 
                |where gene_name = :gene_name 
                |order by snp_id
                |""".stripMargin(), 
                [gene_name: geneName])
                .collect { it.snp_id } as Set,
            patientVariants: patientVariants,
            haplotypeVariants: haplotypeVariants,
        )
    }

    /** The gene_name that this haplotype matrix is for.
     */
    def geneName
    /** An ordered set of snp_id's, representing the snps for this gene.
     */
    LinkedHashSet snpIds
    /** An iterable over rows of patient_id, physical_chromosome, het_combo, snp_id, allele ordered 
     * by those fields.
     */
    def patientVariants
    /** An iterable over rows of haplotype_name, snp_id, allele ordered by those fields.
     */
    def haplotypeVariants

    /** (Allele, SnpID) -> { Haplotype }
     * A mapping from variants to the haplotypes that contain them.
     */
    private Map<List, Set> VH
    /** { Haplotype }
     */
    private Set haplotypes

    String toString() {
        def iterAsList = { iter ->
            def xs = []
            iter.each { xs.add(it) }
            return xs
        }
        def j = { xs -> xs.join(',' + String.format('%n')) }
        "GeneHaplotypeMatrix(${j([geneName, snpIds, '[' + j(iterAsList(patientVariants)) + ']', '[' + j(iterAsList(haplotypeVariants)) + ']'])})"
    }

    @EqualsAndHashCode
    @ToString
    static class Haplotype {
        String haplotypeName
    }

    @EqualsAndHashCode
    @ToString
    static class NovelHaplotype {
        String patientId
        String physicalChromosome
        Integer hetCombo
        Integer hetCombos
    }

    /** Iterate over rows of the gene-haplotype matrix (where as a row is a 
     * Haplotype/NovelHaplotype and a list of alleles for snps in snpIds).
     *
     * Haplotype                      | rs1050828 | rs1050829 | rs5030868 | rs137852328 | rs76723693 | rs2230037
     * B (wildtype)                   | C         | T         | G         | C           | A          | G
     * A-202A_376G                    | T         | C         | G         | C           | A          | G
     * A- 680T_376G                   | C         | C         | G         | A           | A          | G
     * A-968C_376G                    | C         | C         | G         | C           | G          | G
     * Mediterranean Haplotype        | C         | T         | A         | C           | A          | A
     * Sample NA22302-1, Chromosome A | T         | T         | G         |             |            | 
     * Sample NA22302-1, Chromosome B | T         | T         | A         |             |            | 
     * Sample NA22302-2, Chromosome A | T         | T         | G         |             |            | 
     * Sample NA22302-2, Chromosome B | T         | T         | G         |             |            | 
     *
     * The "Haplotype ..." header is just for readibility, it isn't actually a row that we 
     * iterate over.
     *
     * Blank allele cells are represented as null's.
     *
     * f is a function that accepts 2 arguments:
     * 1) an instance of Haplotype or NovelHaplotype
     * 2) an iterable of alleles for the snpIds of this gene
     */ 
    def each(Closure f) {

        def alleles = { variants ->
            def snpIdToAllele = variants.inject([:]) { m, variant ->
                m[variant.snp_id] = variant.allele
                m
            }
            return snpIds.collect { it in snpIdToAllele ? snpIdToAllele[it] : null }
        }
        Row.groupBy(haplotypeVariants, ['haplotype_name']).each { variants ->
            f(new Haplotype(haplotypeName: variants[0].haplotype_name), alleles(variants))
        }
        Row.groupBy(patientVariants, ['patient_id', 'physical_chromosome', 'het_combo']).each { variants ->
            def patientId = variants[0].patient_id
            def physicalChromosome = variants[0].physical_chromosome
            def hetCombo = variants[0].het_combo
            def hetCombos = variants[0].het_combos
            f(
                new NovelHaplotype(
                    patientId: patientId,
                    physicalChromosome: physicalChromosome,
                    hetCombo: hetCombo,
                    hetCombos: hetCombos,
                ),
                alleles(variants),
            )
        }

    }

    private void _variantToHaplotypeSets() {
        /* Initialize VH.
         */
        VH = [:]
        haplotypes = [] as Set
        this.each { haplotype, alleles ->
            if (haplotype instanceof Haplotype) {
                haplotypes.add(haplotype.haplotypeName)
                [snpIds as List, alleles].transpose().each { snpId, allele ->
                    VH.get([snpId, allele], [] as Set).add(haplotype.haplotypeName)
                }
            }
        }
    }

    /** Given an collection of variants (rows with allele and snp_id) belonging to the same physical 
     * chromomsome, return the set of possible haplotypes it contains for this gene.
     * We return null if there isn't at least one variant in variants with a snpId belonging to this 
     * gene.
     */
    def Set variantsToHaplotypes(Collection variants) {
        if (VH == null) {
            _variantToHaplotypeSets()
        }

        /* Whether there is at least one v in variants with snp_id a subset of this gene's snpIds
         */
        boolean hasAtLeastOneSnp = false
        Set haps = new LinkedHashSet(haplotypes)
        for (v in variants) {
            boolean geneContainsSnp = snpIds.contains(v.snp_id)
            hasAtLeastOneSnp = hasAtLeastOneSnp || geneContainsSnp
            Set H = VH[[v.snp_id, v.allele]]
            if (H != null) {
                haps.retainAll(H)
                if (haps.size() == 0) {
                    /* It's a novel haplotype, made novel by a combination of (snp_id, allele) all 
                     * found in other haplotypes, but not in this particular combination.
                     */
                    return haps
                }
            } else if (geneContainsSnp) {
                /* It's a novel haplotype, made novel by a snp_id for this gene with an allele not 
                 * found in any known haplotype.
                 */
                return [] as Set
            }
        }
        if (!hasAtLeastOneSnp) {
            /* There wasn't a single variant whose snp_id was a subset of snpIds; _don't_ return all 
             * the haplotypes for this gene, since that's indicative of there at least being one 
             * variant for the gene but it just being ambiguous which haplotype it belongs to.
             */
            return null
        }
        return haps
    }

}
