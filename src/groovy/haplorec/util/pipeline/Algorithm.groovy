package haplorec.util.pipeline

import haplorec.util.pipeline.Report.GeneHaplotypeMatrix

public class Algorithm {

    static def eachN(int n, iter, Closure f) {
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

    /* hetVariants: [
     *     [snpId: rs1, allele: A],
     *     [snpId: rs1, allele: T],
     *     [snpId: rs2, allele: A],
     *     [snpId: rs2, allele: C],
     *     ...,
     * ]
     */
    static def disambiguateHets(GeneHaplotypeMatrix geneHaplotypeMatrix, hetVariants) {
        def sortedHets = hetVariants.sort(false) { [it.snp_id, it.allele] }
        eachN(2, sortedHets) { h1, h2 ->
            if (h1.snp_id != h2.snp_id) {
                throw new IllegalArgumentException("Expected a list of heterozygote snps (i.e. 2 variants with the same snp_id)")
            }
        }
        def variantToHaplotypes = [:]
        def geneHaplotypes = [] as Set
        geneHaplotypeMatrix.each { haplotype, alleles ->
            geneHaplotypes.add(haplotype.haplotypeName)
            [geneHaplotypeMatrix.snpIds, alleles].transpose().each { snpIdAllele ->
                if (!variantToHaplotypes.containsKey(snpIdAllele)) {
                    variantToHaplotypes[snpIdAllele] = [haplotype.haplotypeName] as Set
                } else {
                    variantToHaplotypes[snpIdAllele].add(haplotype.haplotypeName)
                }
            }
        }

        def hetSnps = sortedHets.collect { it.snp_id }.unique()
        def otherStrand = { AAlleles -> 
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

        /* [A, T] => *1
        /* [T, A] => *2
         * Where key == allele for hetSnps[i]
         *       value == a uniquely determined haplotype for that sequence of SNP's
         */
        def hetSequenceToHaplotype = [:]

        /* TODO: use linked list data structure to prevent inefficient arraylist concatenation
         */
        def uniqueSnps 
        uniqueSnps = { i, variants, sequence, haplotypes ->
            if (i >= variants.size() && haplotypes.size() == 1) {
                hetSequenceToHaplotype[sequence] = haplotypes.iterator().next() 
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
                    uniqueSnps(i+2, variants, sequence + [variant.allele], haps)
                }
                def v1 = variants[i] 
                def v2 = variants[i+1]
                recurse(v1)
                recurse(v2)
            }
        }
        uniqueSnps(0, sortedHets, [], geneHaplotypes)

        def hetSequences = new LinkedHashSet(hetSequenceToHaplotype.keySet())
        def numAnswers = 0
        def s1
        def s2
        while (hetSequences.size() > 0) {
            def s = hetSequences.iterator().next()
            hetSequences.remove(s)
            def sOther = otherStrand(s)
            if (hetSequences.contains(sOther)) {
                assert s1 == null
                assert s2 == null
                s1 = s
                s2 = sOther
                hetSequences.remove(sOther)
            }
        }
        if (s1 != null && s2 != null) {
            def asVariants = { physicalChromosome, alleles ->
                [alleles, hetSnps].transpose().collect { allele, snpId -> 
                    [
                        allele: allele,
                        snp_id: snpId,
                        physical_chromosome: physicalChromosome,
                    ]
                }
            }
			def variants = asVariants('A', s1)
            variants.addAll(asVariants('B', s2))
            return variants
        } else {
            return null
        }
    }

}
