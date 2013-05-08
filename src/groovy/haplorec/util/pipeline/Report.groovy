package haplorec.util.pipeline

import haplorec.util.Row

public class Report {

    static def phenotypeDrugRecommendationReport(Map kwargs = [:], groovy.sql.Sql sql) {
        drugRecommendationReport(kwargs, kwargs.phenotypeDrugRecommendation, sql)
    }

    static def genotypeDrugRecommendationReport(Map kwargs = [:], groovy.sql.Sql sql) {
        drugRecommendationReport(kwargs, kwargs.genotypeDrugRecommendation, sql)
    }

    /* Return an iterable over a list of rows, where the first row is a header.
     */
    private static def drugRecommendationReport(Map kwargs = [:], drugRecommendationTable, groovy.sql.Sql sql) {
        if (kwargs.fillWith == null) {
            // kwargs.fillWith = null is the default
        }
        /* TODO: generate these fields using table metadata queried using the Sql module
         */
        def aliasToColumns = [
            dr   : ['id', 'job_id', 'patient_id', 'drug_recommendation_id'],
            gprd : ['gene_name', 'phenotype_name', 'drug_recommendation_id'],
            gp   : ['gene_name', 'haplotype_name1', 'haplotype_name2', 'phenotype_name'],
            ghv  : ['gene_name', 'haplotype_name', 'snp_id', 'allele'],
        ]
        Set columnsSeen = []
        def col = { alias ->
            def cs = []
            def cols = aliasToColumns[alias]
            cols.each { col -> 
                if (!columnsSeen.contains(col)) {
                    columnsSeen.add(col)
                    cs.add "$alias.$col"
                }
            }
            return cs
        }
        def query = """
        |select
        |
        |${ aliasToColumns.keySet().collect { alias -> col(alias).join(', ') }.join(',\n') }
        |
        |from ${drugRecommendationTable} dr
        |join gene_phenotype_drug_recommendation gprd using (drug_recommendation_id)
        |join genotype_phenotype gp using (gene_name, phenotype_name)
        |join gene_haplotype_variant ghv on (ghv.gene_name = gp.gene_name and ghv.haplotype_name = gp.haplotype_name1) or
        |                                   (ghv.gene_name = gp.gene_name and ghv.haplotype_name = gp.haplotype_name2)
        |
        |where job_id = :job_id
        |""".stripMargin()[0..-2]
        def rows = new Object() {
            def each(Closure f) {
                if (kwargs.sqlParams != null && kwargs.sqlParams != [:]) {
                    sql.eachRow(query, kwargs.sqlParams, f)
                } else {
                    sql.eachRow(query, f)
                }
            }
        }

        Row.fill(with: kwargs.fillWith, (Row.collapse(
            // NOTE: Row.collapse assumes ordering of rows is "correct"; that is, consecutive rows 
            // that satisfy 'canCollapse' are actually meant to be collapsed.  To enforce this we 
            // could add an ORDER BY clause to our query but _I_think_ the fetch order ensures it. 
            // I really ought to double check this...
            Row.noDuplicates(rows,
                // GroupName : [[DuplicateKey], [ColumnsToShow]]
                [
                    // TODO: replace duplicate keys with primary keys from table metadata
                    ( drugRecommendationTable )        : [['id'], ['patient_id', 'drug_recommendation_id']],
                    gene_phenotype_drug_recommendation : [['gene_name', 'phenotype_name', 'drug_recommendation_id'], ['gene_name', 'phenotype_name']],
                    genotype_phenotype                 : [['gene_name', 'haplotype_name1', 'haplotype_name2'], ['haplotype_name1', 'haplotype_name2']],
                    gene_haplotype_variant             : [['gene_name', 'haplotype_name', 'snp_id', 'allele'], ['haplotype_name', 'snp_id', 'allele']],
                ]
            ),
            canCollapse: { header, lastRow, currentRow ->
                /* We can collapse two rows if:
                 * 1. either of them are empty
                 * 2. their columns do not overlap
                 * 3. the index (into the header) of the first column that occurs in currentRow is 
                 *    after the index of the last column in lastRow
                 */

                // 1.
                if (lastRow == [:] || currentRow == [:]) { return true }

                // 2.
                def intersect = new HashSet(lastRow.keySet())
                intersect.retainAll(currentRow.keySet())
                if (intersect.size() != 0) { return false }

                // 3.
                def idx = { column ->
                    def i = 0
                    for (h in header) { 
                        if (h == column) {
                            return i
                        }
                        i += 1
                    }
                }
                // NOTE: 1. guarantees first and last will get set
                def first
                for (entry in currentRow) {
                    first = entry.key
                    break
                }
                def last
                for (entry in lastRow) {
                    last = entry.key
                }
                return idx(first) > idx(last)

            },
        )))
    }

}
