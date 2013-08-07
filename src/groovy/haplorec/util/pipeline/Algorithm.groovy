package haplorec.util.pipeline

import haplorec.util.data.GeneHaplotypeMatrix

/** Algorithm's used in haplorec, from various stages.  
 *
 * The main point of this file to leave out any sql dependencies, making for faster and easier testing.
 */
public class Algorithm {

    /** Given a gene-haplotype matrix for a gene, and heterozygous variants belonging to SNPs of that 
     * gene, disambiguate on which physical chromosome the heterozygote SNPs occur.  
     *
     * This is done by looking for existing haplotypes whose alleles are in the heterozygote calls, 
     * then grouping those alleles on the same physical chromosome that would make that haplotype 
     * call possible.  
     *
     * We are interested in identifying two types of heterozygote call "distributions":
     * 1) AKnownBKnown: 
     *    The heterzygote variants can be distributed on A and B physical chromosomes 
     *    such that they identify 2 existing haplotypes. 
     * 2) AKnownBNovel: 
     *    The heterzygote variants can be distributed on A and B physical chromosomes such that they 
     *    identify 1 existing haplotype on A, and the remaining alleles (after having assigned 
     *    alleles to A) make up a novel haplotype.
     *
     * There is also a third case of ANovelBNovel that we could call, but that behaviour is not 
     * implemented.
     *
     * There may be multiple combinations of answers available for 1) and 2).  We return all 
     * possible combinations of these 2 types.
     *
     * For example:
     *
     * hetVariants: 
     * [
     *     // rs1 AT
     *     [snp_id: rs1, allele: A],
     *     [snp_id: rs1, allele: T],
     *     // rs2 AT
     *     [snp_id: rs2, allele: A],
     *     [snp_id: rs2, allele: T],
     * ]
     *
     * Gene-haplotype matrix:
     * Haplotype | rs1 | rs2
     * *1        | A   | T
     * *2        | T   | A
     * *3        | A   | A
     * *4        | T   | T
     *
     * Returns:
     * [
     *     AKnownBKnown: [
     *         [
     *             [physical_chromosome: 'A', snp_id: 'rs1', allele: 'A'],
     *             [physical_chromosome: 'A', snp_id: 'rs1', allele: 'A'],
     *             [physical_chromosome: 'B', snp_id: 'rs2', allele: 'T'],
     *             [physical_chromosome: 'B', snp_id: 'rs2', allele: 'T'],
     *         ],
     *         [
     *             [physical_chromosome: 'A', snp_id: 'rs1', allele: 'A'],
     *             [physical_chromosome: 'A', snp_id: 'rs1', allele: 'T'],
     *             [physical_chromosome: 'B', snp_id: 'rs2', allele: 'T'],
     *             [physical_chromosome: 'B', snp_id: 'rs2', allele: 'A'],
     *         ],
     *     ]
     *     AKnownBNovel: [
     *        // similar to AKnownBKnown, but it's an empty list in this case
     *     ]
     * ]
     */
    static def disambiguateHets(GeneHaplotypeMatrix geneHaplotypeMatrix, hetVariants) {
        /* Check for valid input.
         */
        hetVariants.each { row ->
            if (!geneHaplotypeMatrix.snpIds.contains(row.snp_id)) {
                throw new IllegalArgumentException("The gene-haplotype matrix for ${geneHaplotypeMatrix.geneName} has no SNP ${row.snp_id}")
            }
        }
        hetVariants.countBy() { row -> row.snp_id }.each { snpId, count ->
            if (count != 2) {
                throw new IllegalArgumentException("Expected a list of heterozygote snps (i.e. 2 variants with the same snp_id), but saw $snpId with $count variants")
            }
        }

        List sortedHets = hetVariants.sort(false) { [it.snp_id, it.allele] }

        /* Allele -> { HaplotypeName }
         * A mapping from an alleles to the haplotypes (in geneHaplotypeMatrix) that contain them.
         */
        Map variantToHaplotypes = [:]
        /* { HaplotypeName }
         * Set of haplotypes (in geneHaplotypeMatrix).
         */
        Set geneHaplotypes = new LinkedHashSet()
        geneHaplotypeMatrix.each { haplotype, alleles ->
            geneHaplotypes.add(haplotype.haplotypeName)
            [geneHaplotypeMatrix.snpIds as List, alleles].transpose().each { snpIdAllele ->
                if (!variantToHaplotypes.containsKey(snpIdAllele)) {
                    variantToHaplotypes[snpIdAllele] = [haplotype.haplotypeName] as Set
                } else {
                    variantToHaplotypes[snpIdAllele].add(haplotype.haplotypeName)
                }
            }
        }

        List hetSnps = sortedHets.collect { it.snp_id }.unique() as List
        int numHets = hetSnps.size()
        /* Given a sequence of alleles made up of variants from sortedHets, return the strand that 
         * would make up the opposing strand (i.e. the remaining variants in sortedHets not already 
         * used in AAlleles).
         */
        Closure<List<CharSequence>> otherStrand = { List<CharSequence> AAlleles -> 
            def BAlleles = new ArrayList(AAlleles.size())
            def i = 0
            eachN(2, sortedHets) { h1, h2 ->
                if (AAlleles[i] == h1.allele) {
                    BAlleles[i] = h2.allele
                } else {
                    assert AAlleles[i] == h2.allele
                    BAlleles[i] = h1.allele
                }
                i += 1
            }
            return BAlleles
        }

        /* [A, T]
        /* [T, A]
         * Where key == sequence of allele[i] for hetSnps[i]
         */
        Set<List<CharSequence>> hetSequences = new LinkedHashSet()

        def uniqueSnps 
        /* Fill hetSequences with heterozygous allele sequences s = ['A', 'T', ...] (where s[i] is 
         * an allele for SNP hetSnp[i]) that uniquely identify a haplotype in geneHaplotypeMatrix.
         */
        uniqueSnps = { int i, List variants, HetSequence sequence, Set haplotypes ->
            if (i >= variants.size() && (
                    /* A unique haplotype is identified by this heterozygote sequence.
                     */
                    haplotypes.size() == 1 || (
                        /* hetVariants has only 1 snp_id; it's something like:
                         * [snp_id: rs1, allele: A], => known haplotype
                         * [snp_id: rs1, allele: T], => novel haplotype
                         * This is a special case, since given only 1 heterozygote call for a gene's SNP, we can 
                         * arbitrarily put each allele on chromosome A or B, regardless of what haplotypes have 
                         * for those alleles.
                         */
                        variants.size() == 2 && 
                        haplotypes.size() > 0
                    ) 
                )
            ) {
                List alleles = sequence.alleles(numHets)
                hetSequences.add(alleles)
            } else if (haplotypes.size() == 0) {
                /* No known haplotype with this sequence.
                 */
                return
            } else if (i >= variants.size()) {
                /* No more variants left to disambiguate remaining haplotypes.
                 */
                return
            } else {
                def recurse = { variant ->
                    Set haps = new LinkedHashSet(haplotypes)
                    /* Only retain known haplotypes which also have the next variant. 
                     * If it's a novel variant, we stop (i.e. Set.retainAll([]) == {}).
                    */
					Set retain = variantToHaplotypes[[variant.snp_id, variant.allele]]
                    haps.retainAll(retain ?: [])
                    uniqueSnps(i+2, variants, new HetSequence(allele: variant.allele, rest: sequence), haps)
                }
                def v1 = variants[i] 
                def v2 = variants[i+1]
                recurse(v1)
                recurse(v2)
            }
        }
        uniqueSnps(0, sortedHets, null, geneHaplotypes)

        /* List of sequences ['A', 'T', ...] for physical chromosomes A and B, where both identify 
         * known haplotypes.
         */
        List<List<CharSequence>> A = []
        List<List<CharSequence>> B = []
        /* Same as above, but where A is known and B is novel.
         */
        List<List<CharSequence>> AKnown = []
        List<List<CharSequence>> BNovel = []
        while (hetSequences.size() > 0) {
            def s = hetSequences.iterator().next()
            hetSequences.remove(s)
            def sOther = otherStrand(s)
            if (hetSequences.contains(sOther)) {
                /* s identifies a known haplotype and so does sOther.  
                 */
                hetSequences.remove(sOther)
                /* Return physical chromosome sequences in a consistent ordering (ordered by 
                 * alleles, with physical chromosome A having the least ordered sequence).
                 */
                def (s1, s2) = [s, sOther].sort()
                A.add(s1)
                B.add(s2)
            } else {
                /* s identifies a known haplotype, but sOther identifies a novel haplotype.  
                 */
                AKnown.add(s)
                BNovel.add(sOther)
            }
        }
        assert A.size() == B.size()
        assert AKnown.size() == BNovel.size()

        /* Annotate a pair of hetSequence's with its snp_id and physical_chromosome.
         * e.g. 
         * hetSnps = ['rs1', 'rs2']
         * s1 = ['A', 'T']
         * s2 = ['G', 'C']
         *
         * Returns: [
         *     [physical_chromosome: 'A': snp_id: 'rs1', allele: 'A'],
         *     [physical_chromosome: 'A': snp_id: 'rs2', allele: 'T'],
         *     [physical_chromosome: 'B': snp_id: 'rs1', allele: 'G'],
         *     [physical_chromosome: 'B': snp_id: 'rs2', allele: 'C'],
         * ]
         */
        def variants = { List<CharSequence> s1, List<CharSequence> s2 ->
            def asVariants = { physicalChromosome, alleles ->
                [alleles, hetSnps].transpose().collect { allele, snpId -> 
                    [
                        physical_chromosome: physicalChromosome,
                        snp_id: snpId,
                        allele: allele,
                    ]
                }
            }
            def vars = asVariants('A', s1)
            vars.addAll(asVariants('B', s2))
            return vars
        }
        def pairsAsRows = { aSequences, bSequences ->
            /* Return pairs of possible sequences in a consistent ordering (order by alleles, ordered by first sequence 
             * then second sequence).
             */
            [aSequences, bSequences].transpose().sort().collect(variants)
        }
        return [
            AKnownBKnown: pairsAsRows(A, B),
            AKnownBNovel: pairsAsRows(AKnown, BNovel),
        ]

    }
    /* Helper class for disambiguateHets.  Used to build up a tree of heterozygous sequence 
     * possibilities, where nodes are allele's and the path from a leaf node to the root represents 
     * a sequence.
     */
    private static class HetSequence {
        CharSequence allele 
        HetSequence rest

        List<CharSequence> alleles(int size) {
            List<CharSequence> xs = new ArrayList(size) 
            HetSequence node = this
            for (int i = 0; i < size; i++) {
                xs[size-i-1] = node.allele
                node = node.rest
            }
            return xs
        }

        /* Useful for debugging.
         */
        List _allelesSoFar() {
            def xs = []
            HetSequence node = this
            while (node != null) {
                xs.add(0, node.allele)
                node = node.rest
            }
            return xs
        }

        String toString() {
            _allelesSoFar().toString()
        }
    }

    /* Given an iterable [x1, x2, ..., xm],  for n = 2 for example, do the computation:
     * f(x1, x2)
     * f(x3, x4)
     * ...
     * f(x(m-1), xm)
     */
    private static def eachN(int n, iter, Closure f) {
        def xs = [] 
        iter.each { x ->
            xs.add(x)
            if (xs.size() == n) {
                f(*xs)
                xs = []
            }
        }
        if (xs != []) {
            /* Fill the rest with nulls.
             */ 
            def ys = xs + ([null] * ( n - xs.size() ))
            f(*ys)
        }
    }

}
