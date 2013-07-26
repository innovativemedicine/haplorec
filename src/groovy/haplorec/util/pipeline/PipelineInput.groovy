package haplorec.util.pipeline

import groovy.lang.Closure;
import haplorec.util.Input

public class PipelineInput {
    // if we see the header, skip it, o/w assume its a row
    static final Boolean defaultRequireHeader = false
    static final Map inputHeaders = [
        variant       : ['PLATE', 'EXPERIMENT', 'CHIP', 'WELL_POSITION', 'ASSAY_ID', 'GENOTYPE_ID', 'DESCRIPTION', 'SAMPLE_ID', 'ENTRY_OPERATOR'],
        genePhenotype : ['SAMPLE_ID', 'GENE', 'PHENOTYPE'],
        genotype      : ['SAMPLE_ID', 'GENE', 'HAPLOTYPE', 'HAPLOTYPE'],
        geneHaplotype : ['SAMPLE_ID', 'GENE', 'HAPLOTYPE'],
    ]

    static final Set inputTables = inputHeaders.keySet()

    /* Given a tableAlias, return a function of the type (Map kwargs = [:], fileOrStream) -> iterator that 
     * returns an row iterator over a file or input stream (kwargs are the same as those defined in 
     * Input.dsv).  This is just a wrapper over Input.dsv.
     */
    static def tableAliasToTableReader(String tableAlias) {
        def tableReader = "${tableAlias}s"
        if (!inputTables.contains(tableAlias)) {
            throw new IllegalArgumentException("no input reader for table ${tableAlias}; try adding it to inputTables and defining PipelineInput.${tableReader}")
        }
		if (PipelineInput.metaClass.respondsTo(PipelineInput, tableReader)) {
			return PipelineInput.&"${tableReader}"
		} else {
			// return a default reader
			return PipelineInput.defaultReader(tableAlias)
		}
    }
	
    /* Default table reader.
     * (NOTE: you need to define inputHeaders for the tableAlias)
     */
	static def defaultReader(String tableAlias) {
        return { Map kwargs = [:], fileOrStream ->
            return Input.dsv(
                // overridable kwargs to Input.dsv 
                [:] +
                kwargs + 
                // non-overridable kwargs to Input.dsv 
                [
                    asList: true,
                    header: inputHeaders[tableAlias],
                    requireHeader: defaultRequireHeader,
                ], fileOrStream)
        }
	}

    /* Table specific iterators  
     * (NOTE: the function name must be the table alias plus an 's' to be recognized by tableAliasToTableReader) 
     */

	static def variants(Map kwargs = [:], fileOrStream) {
		if (kwargs.asList == null) { kwargs.asList = true }
		if (kwargs.skipEmptyAlleles == null) { kwargs.skipEmptyAlleles = false }
        def iter = Input.dsv(kwargs + [
            header: inputHeaders.variant,
            requireHeader: defaultRequireHeader,
            fields: ['ASSAY_ID', 'GENOTYPE_ID', 'SAMPLE_ID'],
            asList: false,
        ], fileOrStream)
        return new Object() {
            def each(Closure f) {
                def chromosomes = ['A', 'B']
                iter.each { snpId, allelesStr, patientId -> 
                    def zygosity 
                    def alleles
                    if (allelesStr.length() == 2) {
                        zygosity = 'het'
                        alleles = allelesStr.collect()
                    } else if (allelesStr.length() == 1) {
                        zygosity = 'hom'
                        alleles = allelesStr.collect() * 2
                    } else if (allelesStr.length() == 0) {
                        alleles = ['']
                    } else {
                        /* Given a variant like "rs1 CAT", we assume that means:
                         *
                         * chromosome A:
                         * rs1 CAT 
                         *
                         * chromosome B:
                         * rs1 CAT 
                         */
                        // TOOD: zygosity could be het (Swan said something about CTTdel being heterozygous)
                        zygosity = 'hom'
                        alleles = [allelesStr] * 2
                    }
                    int i = 0
                    alleles.each { allele ->
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
