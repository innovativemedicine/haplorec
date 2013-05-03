package haplorec.util.haplotype

import groovy.lang.Closure;
import haplorec.util.Input

public class HaplotypeInput {
    static Set inputTables = [
        'variant',
        'genePhenotype',
        'genotype',
        'geneHaplotype',
    ] as Set

    /* Given a tableAlias (
     */
    static def tableAliasToTableReader(String tableAlias) {
        def tableReader = "${tableAlias}s"
        if (!inputTables.contains(tableAlias)) {
            throw new IllegalArgumentException("no input reader for table ${tableAlias}; try adding it to inputTables and defining HaplotypeInput.${tableReader}")
        }
		if (HaplotypeInput.metaClass.respondsTo(HaplotypeInput, tableReader)) {
			return HaplotypeInput.&"${tableReader}"
		} else {
			// return a default reader
			return HaplotypeInput.&defaultReader
		}
    }
	
	static def defaultReader(Map kwargs = [:], fileOrStream) {
		return Input.dsv(fileOrStream,
			asList: true,
		)
	}

	static def variants(Map kwargs = [:], fileOrStream) {
		if (kwargs.asList == null) { kwargs.asList = true }
		if (kwargs.skipEmptyAlleles == null) { kwargs.skipEmptyAlleles = false }
        def iter = Input.dsv(kwargs + [
            header: ['PLATE', 'EXPERIMENT', 'CHIP', 'WELL_POSITION', 'ASSAY_ID', 'GENOTYPE_ID', 'DESCRIPTION', 'SAMPLE_ID', 'ENTRY_OPERATOR'],
            fields: ['ASSAY_ID', 'GENOTYPE_ID', 'SAMPLE_ID'],
            asList: false,
        ], fileOrStream)
        return new Object() {
            def each(Closure f) {
                def chromosomes = ['A', 'B']
                iter.each { snpId, alleles, patientId -> 
                    def zygosity 
                    if (alleles.length() == 2) {
                        zygosity = 'het'
                    } else if (alleles.length() == 1) {
                        zygosity = 'hom'
                    } else if (alleles.length() != 0) {
                        throw new Input.InvalidInputException("Number of alleles was ${alleles.length()} for ${snpId} ${alleles}; expected 0, 1, or 2".toString())
                    }
                    int i = 0
                    alleles.split('').each { allele ->
                        def checkAllele = { value -> (allele == '') ? null : value } 
                        def chromosome = checkAllele(chromosomes[i])
                        Input.applyF(kwargs.asList, [patientId, chromosome, snpId, checkAllele(allele), checkAllele(zygosity)], f)
                        if (chromosome != null) {
                            i += 1
                        }
                    }
                }
            }
        }
	}

}
