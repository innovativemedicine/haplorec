package haplorec.util.pipeline

import haplorec.util.Row
import haplorec.util.Sql
import static haplorec.util.Sql._

public class Report {

    static def phenotypeDrugRecommendationReport(Map kwargs = [:], groovy.sql.Sql sql) {
        kwargs += Pipeline.tables(kwargs)
        condensedJoin(sql,
            fillWith: kwargs.fillWith,
            select: [
                ( kwargs.phenotypeDrugRecommendation ) : ['patient_id', 'drug_recommendation_id'],
                drug_recommendation                    : ['drug_name', 'recommendation'],
                gene_phenotype_drug_recommendation     : ['gene_name', 'phenotype_name'],
                genotype_phenotype                     : ['haplotype_name1', 'haplotype_name2'],
                gene_haplotype_variant                 : ['haplotype_name', 'snp_id', 'allele'],
            ],
            join: [
                drug_recommendation                : [ "join", "on (jppdr.drug_recommendation_id = dr.id)"],
                gene_phenotype_drug_recommendation : ["join", "using (drug_recommendation_id)"],
                ( kwargs.genePhenotype )           : ["left join", "using (gene_name, phenotype_name) "],
                genotype_phenotype                 : ["join", "using (gene_name, phenotype_name)"],
                ( kwargs.genotype )                : ["left join", "using (haplotype_name1, haplotype_name2) "],
                gene_haplotype_variant             : ["join", "on (ghv.gene_name = jpg.gene_name and ghv.haplotype_name = jpg.haplotype_name1) or (ghv.gene_name = jpg.gene_name and ghv.haplotype_name = jpg.haplotype_name2)"],
                ( kwargs.geneHaplotype )           : ["left join", "on (ghv.gene_name = jpgh.gene_name) and (ghv.haplotype_name = jpgh.haplotype_name)"],
                ( kwargs.variant )                 : ["left join", "using (snp_id, allele)"],
            ],
            where: "jppdr.job_id = :job_id",
            sqlParams: kwargs.sqlParams)
    }

    static def genotypeDrugRecommendationReport(Map kwargs = [:], groovy.sql.Sql sql) {
        kwargs += Pipeline.tables(kwargs)
        condensedJoin(sql,
            fillWith: kwargs.fillWith,
            select: [
                ( kwargs.genotypeDrugRecommendation ) : ['patient_id', 'drug_recommendation_id'],
                drug_recommendation                   : ['drug_name', 'recommendation'],
                genotype_drug_recommendation          : ['gene_name', 'haplotype_name1', 'haplotype_name2'],
                gene_haplotype_variant                : ['haplotype_name', 'snp_id', 'allele'],
            ],
            join: [
                drug_recommendation          : [ "join", "on (jpgdr.drug_recommendation_id = dr.id)"],
                genotype_drug_recommendation : ["join", "using (drug_recommendation_id)"],
                // same as above, minus kwargs.genePhenotype and genotype_phenotype
                ( kwargs.genotype )          : ["left join", "using (haplotype_name1, haplotype_name2) "],
                gene_haplotype_variant       : ["join", "on (ghv.gene_name = jpg.gene_name and ghv.haplotype_name = jpg.haplotype_name1) or (ghv.gene_name = jpg.gene_name and ghv.haplotype_name = jpg.haplotype_name2)"],
                ( kwargs.geneHaplotype )     : ["left join", "on (ghv.gene_name = jpgh.gene_name) and (ghv.haplotype_name = jpgh.haplotype_name)"],
                ( kwargs.variant )           : ["left join", "using (snp_id, allele)"],
            ],
            where: "jpgdr.job_id = :job_id",
            sqlParams: kwargs.sqlParams)
    }

    private static def aliafy(table) {
        table.replaceAll(
            /(:?^|_)(\w)[^_]*/, 
            { 
                it[2].toLowerCase() 
            })
    }

    /* Return an iterable over a list of rows, where the first row is a header.
     */
    private static def condensedJoin(Map kwargs = [:], groovy.sql.Sql sql) {
        if (kwargs.fillWith == null) {
            // kwargs.fillWith = null is the default
        }
        def tables = Sql.tblColumns(sql)

        /* Table name to alias mapping.
         */
        def alias = ( kwargs.select.keySet() + kwargs.join.keySet() ).inject([:]) { m, table -> m[table] = aliafy(table); m }
        /* Alias to table mapping.
         */
        def aliasToTable = alias.keySet().inject([:]) { m, k -> m[alias[k]] = k; m }

        Set columnsSeen = []
        def cols = { table ->
            def columns = []
            tables[table].columns.each { column -> 
                if (!columnsSeen.contains(column)) {
                    columnsSeen.add(column)
                    columns.add "${alias[table]}.$column"
                }
            }
            return columns
        }

        def joinTables = kwargs.join.keySet()
        def tablesNotInJoin = new HashSet(kwargs.select.keySet())
        tablesNotInJoin.removeAll(joinTables)
        if (tablesNotInJoin.size() != 1) {
            throw new IllegalArgumentException("There must be exactly one table without a join clause, but saw ${tablesNotInJoin.size()} such tables: ${tablesNotInJoin}")
        }
        def tableNotInJoin = tablesNotInJoin.iterator().next()
        
        def query = """
        |select
        |${ kwargs.select.keySet().collect { table -> cols(table).join(', ') }.join(',\n') }
        |from 
        |${ ( ["$tableNotInJoin ${alias[tableNotInJoin]}"] + kwargs.join.collect { "${it.value[0]} ${it.key} ${alias[it.key]} ${it.value[1]}" } ).join('\n') }
        |${ _(kwargs.where, return: { "where $it" }) }
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
                kwargs.select.inject([:]) { m, pair ->
                    def (table, columns) = [pair.key, pair.value]
                    m[table] = [tables[table].primaryKey, columns]
                    return m
                }
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
