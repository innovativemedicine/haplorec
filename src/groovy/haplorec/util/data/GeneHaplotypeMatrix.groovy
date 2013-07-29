package haplorec.util.data

import haplorec.util.data.Sql
import haplorec.util.Row

import java.sql.Connection
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

class GeneHaplotypeMatrix {

    /* Return a GeneHaplotypeMatrix annotated with patientVariants for a particular kwargs.job_id.
     */
    static GeneHaplotypeMatrix novelHaplotypeMatrix(Map kwargs = [:], sql, jobId, geneName) {
        if (kwargs.iterableManyTimes == null) { kwargs.iterableManyTimes = true }
        def matrix
        Sql.withConnection(sql) { connection -> 
            def patientVariantsStmt = Sql.stmtIter(connection, """\
                    |select snp_id, allele, patient_id, het_combo, het_combos, physical_chromosome
                    |from ${kwargs.novelHaplotype}
                    |join ${kwargs.variant} using (job_id, patient_id, physical_chromosome)
                    |where job_id = ? and gene_name = ?
                    |order by job_id, gene_name, patient_id, physical_chromosome, het_combo, snp_id
                    |""".stripMargin(),
                    cacheRows: kwargs.iterableManyTimes)
            patientVariantsStmt.execute(jobId, geneName)
            matrix = geneHaplotypeMatrix(kwargs, sql, connection, geneName, patientVariantsStmt)
        }
        return matrix
    }
    
    static GeneHaplotypeMatrix haplotypeMatrix(Map kwargs = [:], sql, geneName) {
        if (kwargs.iterableManyTimes == null) { kwargs.iterableManyTimes = true }
        def matrix
        Sql.withConnection(sql) { connection -> 
            matrix = geneHaplotypeMatrix(kwargs, sql, connection, geneName, null)
        }
        return matrix
    }

    private static GeneHaplotypeMatrix geneHaplotypeMatrix(Map kwargs = [:], sql, connection, geneName, patientVariants) {
        if (kwargs.iterableManyTimes == null) { kwargs.iterableManyTimes = true }
        def haplotypeVariantsStmt = Sql.stmtIter(connection, """
                |select haplotype_name, snp_id, allele
                |from gene_haplotype_variant
                |where gene_name = ?
                |order by gene_name, haplotype_name, snp_id
                |""".stripMargin(),
                cacheRows: kwargs.iterableManyTimes)
        haplotypeVariantsStmt.execute(geneName)
        return new GeneHaplotypeMatrix(
            geneName: geneName,
            snpIds: sql.rows("""
                |select snp_id 
                |from gene_snp 
                |where gene_name = :gene_name 
                |order by snp_id
                |""".stripMargin(), [gene_name: geneName])
                .collect { it.snp_id } as Set,
            patientVariants: patientVariants,
            haplotypeVariants: haplotypeVariantsStmt,
        )
    }

    // The gene_name that this haplotype matrix is for.
    def geneName
    // An ordered set of snp_id's, representing the snps for this gene.
    LinkedHashSet snpIds
    // An iterable over rows of snp_id, allele, patient_id, physical_chromosome, het_combo, het_combos
    // ordered by those fields.
    def patientVariants
    // An iterable over rows of haplotype_name, snp_id, allele
    // ordered by those fields.
    def haplotypeVariants


    // (Allele, SnpID) -> { Haplotype }
    private Map<List, Set> VH
    // { Haplotype }
    private Set haplotypes

    String toString() {
        def j = { xs -> xs.join(',' + String.format('%n')) }
        "GeneHaplotypeMatrix(${j([geneName, snpIds, '[' + j(Sql.iterAsList(patientVariants)) + ']', '[' + j(Sql.iterAsList(haplotypeVariants)) + ']'])})"
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
        int hetCombo
        int hetCombos
    }

    def each(Closure f) {
        /* Iterate over rows of the gene-haplotype matrix, like this:
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
         * The "Haplotype ..." header is just for readibility, it isn't actually a row that 
         * we iterate over.
         *
         * Blank allele cells are represented as null's.
         *
         * f is a function that accepts 2 arguments:
         * 1) an instance of Haplotype or NovelHaplotype
         * 2) an iterable of alleles for the snpIds of this gene
         *
         */ 

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

    /* Given an collection of variants (rows with allele and snp_id), return the set of possible 
     * haplotypes it contains for this gene.
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
                 * found in any haplotype.
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
