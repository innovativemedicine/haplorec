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
                ( kwargs.genePhenotype )               : ['gene_name', 'phenotype_name'],
                ( kwargs.genotype )                    : ['haplotype_name1', 'haplotype_name2'],
                ( kwargs.geneHaplotype )               : ['haplotype_name'],
                ( kwargs.variant )                     : ['snp_id', 'allele'],
            ],
            join: [
                drug_recommendation: [ "left join", "on (jppdr.drug_recommendation_id = dr.id)" ],
                gene_phenotype_drug_recommendation: [ "left join", "using (drug_recommendation_id)" ],
                ( kwargs.genePhenotype )           : ["left join", "using (job_id, patient_id, gene_name, phenotype_name) "],
                genotype_phenotype                 : ["left join", "using (gene_name, phenotype_name)"],
                ( kwargs.genotype ): [ "left join", "using (job_id, patient_id, haplotype_name1, haplotype_name2)" ],
                // ( kwargs.genotype ): [ "left join", """\
                //     on jpg.job_id = jpgp.job_id and
                //        jpg.patient_id = jpgp.patient_id and
                //        jpg.haplotype_name1 = gp.haplotype_name1 and
                //        jpg.haplotype_name2 = gp.haplotype_name2
                //     """ ],
                ( kwargs.geneHaplotype ): [ "left join", """\
                    on (
                        jpgh.job_id = jpg.job_id and
                        jpgh.patient_id = jpg.patient_id and
                        jpgh.gene_name = jpg.gene_name and
                        jpgh.haplotype_name = jpg.haplotype_name1
                       ) or (
                        jpgh.job_id = jpg.job_id and
                        jpgh.patient_id = jpg.patient_id and
                        jpgh.gene_name = jpg.gene_name and
                        jpgh.haplotype_name = jpg.haplotype_name2
                       )""" ],
                gene_haplotype_variant: [ "left join",  """\
                    on ghv.gene_name = jpgh.gene_name and 
                       ghv.haplotype_name = jpgh.haplotype_name
                    """ ],
                ( kwargs.variant ): [ "left join",  """\
                    on jpv.patient_id = jpgh.patient_id and
                       jpv.job_id = jpgh.job_id and
                       jpv.snp_id = ghv.snp_id and
                       jpv.allele = ghv.allele
                    """ ],
                // drug_recommendation                : [ "join", "on (jppdr.drug_recommendation_id = dr.id)"],
                // gene_phenotype_drug_recommendation : ["join", "using (drug_recommendation_id)"],
                // ( kwargs.genePhenotype )           : ["left join", "using (gene_name, phenotype_name) "],
                // genotype_phenotype                 : ["join", "using (gene_name, phenotype_name)"],
                // ( kwargs.genotype )                : ["left join", "using (haplotype_name1, haplotype_name2) "],
                // gene_haplotype_variant             : ["join", "on (ghv.gene_name = jpg.gene_name and ghv.haplotype_name = jpg.haplotype_name1) or (ghv.gene_name = jpg.gene_name and ghv.haplotype_name = jpg.haplotype_name2)"],
                // ( kwargs.geneHaplotype )           : ["left join", "on (ghv.gene_name = jpgh.gene_name) and (ghv.haplotype_name = jpgh.haplotype_name)"],
                // ( kwargs.variant )                 : ["left join", "using (snp_id, allele)"],
            ],
            where: "jppdr.job_id = :job_id",
            sqlParams: kwargs.sqlParams,
            duplicateKey: [
                // don't repeat a haplotype for the same patient
                ( kwargs.geneHaplotype ) : ['job_id', 'patient_id', 'gene_name', 'haplotype_name'],
                // repeat variants for the same patient but a different haplotype (but we don't 
                // repeat variants with the same allele and snp_id but only a different zygosity)
                ( kwargs.variant )       : ['job_id', 'patient_id', [ ( kwargs.geneHaplotype ) : [ 'gene_name', 'haplotype_name' ] ], 'allele', 'snp_id'],
            ])
    }

    static def genotypeDrugRecommendationReport(Map kwargs = [:], groovy.sql.Sql sql) {
        kwargs += Pipeline.tables(kwargs)
        condensedJoin(sql,
            fillWith: kwargs.fillWith,
            select: [
                ( kwargs.genotypeDrugRecommendation ) : ['patient_id', 'drug_recommendation_id'],
                drug_recommendation                   : ['drug_name', 'recommendation'],
                ( kwargs.genotype )                   : ['gene_name', 'haplotype_name1', 'haplotype_name2'],
                ( kwargs.geneHaplotype )              : ['haplotype_name'],
                ( kwargs.variant )                    : ['snp_id', 'allele'],
            ],
            join: [
                drug_recommendation: [ "left join", "on (jpgdr.drug_recommendation_id = dr.id)" ],
                genotype_drug_recommendation: [ "left join", "using (drug_recommendation_id)" ],
                ( kwargs.genotype ): [ "left join", "using (job_id, patient_id, haplotype_name1, haplotype_name2)" ],
                ( kwargs.geneHaplotype ): [ "left join", """\
                    on (
                        jpgh.job_id = jpg.job_id and
                        jpgh.patient_id = jpg.patient_id and
                        jpgh.gene_name = jpg.gene_name and
                        jpgh.haplotype_name = jpg.haplotype_name1
                       ) or (
                        jpgh.job_id = jpg.job_id and
                        jpgh.patient_id = jpg.patient_id and
                        jpgh.gene_name = jpg.gene_name and
                        jpgh.haplotype_name = jpg.haplotype_name2
                       )""" ],
                gene_haplotype_variant: [ "left join",  """\
                    on ghv.gene_name = jpgh.gene_name and 
                       ghv.haplotype_name = jpgh.haplotype_name
                    """ ],
                ( kwargs.variant ): [ "left join",  """\
                    on jpv.patient_id = jpgh.patient_id and
                       jpv.job_id = jpgh.job_id and
                       jpv.snp_id = ghv.snp_id and
                       jpv.allele = ghv.allele
                    """ ],

                // drug_recommendation          : ["join", "on (jpgdr.drug_recommendation_id = dr.id)"],
                // genotype_drug_recommendation : ["join", "using (drug_recommendation_id)"],
                // // same as above, minus kwargs.genePhenotype and genotype_phenotype"""
                // ( kwargs.genotype )          : ["left join", "using (haplotype_name1, haplotype_name2) "],
                // gene_haplotype_variant       : ["join", "on (ghv.gene_name = jpg.gene_name and ghv.haplotype_name = jpg.haplotype_name1) or (ghv.gene_name = jpg.gene_name and ghv.haplotype_name = jpg.haplotype_name2)"],
                // ( kwargs.geneHaplotype )     : ["left join", "on (ghv.gene_name = jpgh.gene_name) and (ghv.haplotype_name = jpgh.haplotype_name)"],
                // ( kwargs.variant )           : ["left join", "using (snp_id, allele)"],
            ],
            where: "jpgdr.job_id = :job_id",
            sqlParams: kwargs.sqlParams,
            duplicateKey: [
                // don't repeat a haplotype for the same patient
                ( kwargs.geneHaplotype ) : ['job_id', 'patient_id', 'gene_name', 'haplotype_name'],
                // repeat variants for the same patient but a different haplotype (but we don't 
                // repeat variants with the same allele and snp_id but only a different zygosity)
                ( kwargs.variant )       : ['job_id', 'patient_id', [ ( kwargs.geneHaplotype ) : [ 'gene_name', 'haplotype_name' ] ], 'allele', 'snp_id'],
            ])
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
        if (kwargs.duplicateKey == null) {
            kwargs.duplicateKey = [:]
        }
        def tables = Sql.tblColumns(sql)

        /* Table name to alias mapping.
         */
        def alias = ( kwargs.select.keySet() + kwargs.join.keySet() ).inject([:]) { m, table -> m[table] = aliafy(table); m }
        /* Alias to table mapping.
         */
        def aliasToTable = alias.keySet().inject([:]) { m, k -> m[alias[k]] = k; m }

        def joinTables = kwargs.join.keySet()
        def tablesNotInJoin = new HashSet(kwargs.select.keySet())
        tablesNotInJoin.removeAll(joinTables)
        if (tablesNotInJoin.size() != 1) {
            throw new IllegalArgumentException("There must be exactly one table without a join clause, but saw ${tablesNotInJoin.size()} such tables: ${tablesNotInJoin}")
        }
        def tableNotInJoin = tablesNotInJoin.iterator().next()

        // |${ kwargs.select.keySet().collect { table -> tables[table].columns.collect { "${alias[table]}.$it" }.join(',\n') }
        def query = """
        |select
        |${ kwargs.select.keySet().collect { table -> tables[table].columns.collect { "${alias[table]}.$it" }.join(', ') }.join(',\n') }
        |from 
        |${ ( ["$tableNotInJoin ${alias[tableNotInJoin]}"] + kwargs.join.collect { "${it.value[0]} ${it.key} ${alias[it.key]} ${it.value[1]}" } ).join('\n') }
        |${ _(kwargs.where, return: { "where $it" }) }
        |""".stripMargin()[0..-2]

        println "----"
        println query
        println "----"

        def colName = { table, column ->
            "$table.$column".toString()
        }

        def rows = Sql.rows(sql, query, 
            sqlParams: kwargs.sqlParams,
            names: kwargs.select.keySet().collect { table -> 
                tables[table].columns.collect { c -> colName(table, c) } 
            }.flatten(),
        )

        Row.fill(
            with: kwargs.fillWith, 
            // NOTE: Row.collapse assumes ordering of rows is "correct"; that is, consecutive rows 
            // that satisfy 'canCollapse' are actually meant to be collapsed.  To enforce this we 
            // could add an ORDER BY clause to our query but _I_think_ the fetch order ensures it. 
            // I really ought to double check this...
            Row.collapse(
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
                Row.filter(
                    keep: kwargs.select.collect { kv -> 
                        def (table, columns) = [kv.key, kv.value] 
                        columns.collect { c -> colName(table, c) } 
                    }.flatten(),
                    Row.noDuplicates(rows,
                        // GroupName : [[DuplicateKey], [ColumnsToShow]]
                        kwargs.select.inject([:]) { m, pair ->
                            def (table, columns) = [pair.key, pair.value]
                            def dupkey = (kwargs.duplicateKey[table] != null) ? 
                                kwargs.duplicateKey[table] :
                                tables[table].primaryKey
                            m[table] = [
                                dupkey.collect { c -> 
                                    if (c instanceof Map) {
                                        def tbl = c.keySet().iterator().next()
                                        c[tbl].collect { colName(tbl, it) }
                                    } else {
                                        colName(table, c) 
                                    }
                                }.flatten(),
                                columns.collect { c -> colName(table, c) },
                            ]
                            return m
                        }
                    ),
                )
            )
        )
    }

}
