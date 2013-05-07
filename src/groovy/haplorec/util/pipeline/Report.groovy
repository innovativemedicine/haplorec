package haplorec.util.pipeline

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
        // return rows

        // TODO: 
        noDuplicates(rows,
            // GroupName : [[DuplicateKey], [ColumnsToShow]]
            [
                // TODO: replace duplicate keys with primary keys from table metadata
                ( drugRecommendationTable )        : [['id'], ['patient_id', 'drug_recommendation_id']],
                gene_phenotype_drug_recommendation : [['gene_name', 'phenotype_name', 'drug_recommendation_id'], ['gene_name', 'phenotype_name']],
                genotype_phenotype                 : [['gene_name', 'haplotype_name1', 'haplotype_name2'], ['haplotype_name1', 'haplotype_name2']],
                gene_haplotype_variant             : [['gene_name', 'haplotype_name', 'snp_id', 'allele'], ['haplotype_name', 'snp_id', 'allele']],
            ],
            fillDuplicate: kwargs.fillDuplicate,
        )

    }

    private static def noDuplicates(Map kwargs = [:], iter, groups) {
        return new Object() {
            def each(Closure f) {
                Map seen = groups.keySet().inject([:]) { m, g -> m[g] = [] as Set; m }
                iter.each { map ->
                    def row = [:]
                    groups.each { g, groupColumns ->
                        def (duplicateKey, columnsToShow) = groupColumns
                        def k = duplicateKey.collect { map[it] }
                        if (!seen[g].contains(k)) {
                            // add the group g
                            seen[g].add(k)
                            columnsToShow.each { row[it] = map[it] }
                        } else if (kwargs.fillDuplicate != null) {
                            columnsToShow.each { kwargs.fillDuplicate(row, map, it) }
                        }
                    }
                    f(row)
                }
            }
        }
    }

    private static def asDSV(Map kwargs = [:], iter, Appendable stream) {
        if (kwargs.separator == null) { kwargs.separator = '\t' }
        if (kwargs.null == null) { 
            kwargs.null = { v -> 
                (v == null) ? '' : v.toString()
            }
        }
        def header
        def i = 0
        def output = { r ->
            stream.append(r.collect() { kwargs.null(it) }.join(kwargs.separator))
            stream.append(System.getProperty("line.separator"))
            // r.each {
            //     stream.append()
            // }
        }
        iter.each { row ->
            if (i == 0) {
                header = row.keySet()
                output(header)
            }
            output(header.collect { row[it] })
        }
    }

}
