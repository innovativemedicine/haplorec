package haplorec.util.pipeline

import haplorec.util.Row
import haplorec.util.data.GeneHaplotypeMatrix
// import haplorec.util.sql.Report (we use this, but we can't import it due to conflicting names)

/** Generate reports suitable for output to DSV format.
 */
public class Report {

    /** Given a job_id, return an iterator over GeneHaplotypeMatrix's, 1 for each distinct gene identified in 
     * novelHaplotype (for this job).
     *
     * @param sql a connection to the haplorec database
     * @param kwargs.sqlParams.job_id a job_id
     */
    static def novelHaplotypeReport(Map kwargs = [:], groovy.sql.Sql sql) {
        kwargs += Pipeline.tables(kwargs)
        if (kwargs.iterableManyTimes == null) { kwargs.iterableManyTimes = false }
        return new Object() {
            def each(Closure f) {
                def geneNames = sql.rows("""
                    |select distinct gene_name 
                    |from ${kwargs.novelHaplotype} 
                    |where job_id = :job_id 
                    |order by gene_name
                    |""".stripMargin(), kwargs.sqlParams)
                    .collect { it.gene_name }
                geneNames.each { geneName ->
                    f(GeneHaplotypeMatrix.novelHaplotypeMatrix(sql, kwargs.sqlParams.job_id, geneName))
                }
            }
        }
    }

    /** Given a job_id, return a report showingevery phenotype-based drug recommendation was called.
     * In particular, show:
     * - the drug recommendation details                    (kwargs.phenotypeDrugRecommendation / drug_recommendation)  
     * - the gene phenotypes that caused the recommendation (kwargs.genePhenotype) 
     * - the genotype that caused each gene phenotype       (kwargs.genotype)      
     * - the haplotypes that cause each genotype            (kwargs.geneHaplotype) 
     * - the variants that caused each haplotype call       (kwargs.variant)       
     * Hence, it's just one big join.  We also don't want to show repeated data for the same 
     * patient_id, so we use Row.condensedJoin to filter out duplicate data (specified via 
     * duplicateKey). 
     *
     * Returns:
     * an iterable of maps, all containing the attributes in "select: [ ... ]" below, aliased using 
     * aliases defined in Report.report(...).
     *
     * @param sql a connection to the haplorec database
     * @param kwargs.sqlParams.job_id a job_id
     */
    static def phenotypeDrugRecommendationReport(Map kwargs = [:], groovy.sql.Sql sql) {
        kwargs += Pipeline.tables(kwargs)
        report(sql,
            fillWith: kwargs.fillWith,
            select: [
                ( kwargs.phenotypeDrugRecommendation ) : ['patient_id', 'drug_recommendation_id', 'het_combo', 'het_combos'],
                drug_recommendation                    : ['drug_name', 'recommendation'],
                ( kwargs.genePhenotype )               : ['gene_name', 'phenotype_name'],
                ( kwargs.genotype )                    : ['haplotype_name1', 'haplotype_name2'],
                ( kwargs.geneHaplotype )               : ['haplotype_name'],
                ( kwargs.variant )                     : ['snp_id', 'allele'],
            ],
            join: [
                drug_recommendation: [ "left join", "on (jppdr.drug_recommendation_id = dr.id)" ],
                gene_phenotype_drug_recommendation: [ "left join", "using (drug_recommendation_id)" ],
                ( kwargs.genePhenotype )           : ["left join", "using (job_id, patient_id, gene_name, phenotype_name, het_combo) "],
                genotype_phenotype                 : ["left join", "using (gene_name, phenotype_name)"],
                ( kwargs.genotype ): [ "left join", "using (job_id, patient_id, haplotype_name1, haplotype_name2, het_combo)" ],
                ( kwargs.geneHaplotype ): [ "left join", """\
                    on (
                        jpgh.job_id = jpg.job_id and
                        jpgh.patient_id = jpg.patient_id and
                        jpgh.gene_name = jpg.gene_name and
                        jpgh.haplotype_name = jpg.haplotype_name1 and
                        jpgh.het_combo = jpg.het_combo
                       ) or (
                        jpgh.job_id = jpg.job_id and
                        jpgh.patient_id = jpg.patient_id and
                        jpgh.gene_name = jpg.gene_name and
                        jpgh.haplotype_name = jpg.haplotype_name2 and
                        jpgh.het_combo = jpg.het_combo
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
            ],
            where: "jppdr.job_id = :job_id",
            sqlParams: kwargs.sqlParams,
            duplicateKey: [
                /* Repeat drug recommendation details for different patients
                 */
                drug_recommendation : ['id', [ ( kwargs.phenotypeDrugRecommendation ) : ['job_id', 'patient_id'] ]],
                /* Repeat phenotypes for different drug recommendations
                 */
                ( kwargs.genePhenotype ) : ['id', [ drug_recommendation : ['id'] ]],
                /* Don't repeat a haplotype for the same patient
                 */
                ( kwargs.geneHaplotype ) : ['job_id', 'patient_id', 'gene_name', 'haplotype_name'],
                /* Repeat variants for the same patient but a different haplotype (but we don't 
                 * repeat variants with the same allele and snp_id but only a different zygosity)
                 */
                ( kwargs.variant ) : ['job_id', 'patient_id', [ ( kwargs.geneHaplotype ) : [ 'gene_name', 'haplotype_name' ] ], 'allele', 'snp_id'],
            ])
    }

    /** Same idea as phenotypeDrugRecommendationReport, but starting from kwargs.genotypeDrugRecommendation 
     * instead of kwargs.phenotypeDrugRecommendation.
     */
    static def genotypeDrugRecommendationReport(Map kwargs = [:], groovy.sql.Sql sql) {
        kwargs += Pipeline.tables(kwargs)
        report(sql,
            fillWith: kwargs.fillWith,
            select: [
                ( kwargs.genotypeDrugRecommendation ) : ['patient_id', 'drug_recommendation_id', 'het_combo', 'het_combos'],
                drug_recommendation                   : ['drug_name', 'recommendation'],
                ( kwargs.genotype )                   : ['gene_name', 'haplotype_name1', 'haplotype_name2'],
                ( kwargs.geneHaplotype )              : ['haplotype_name'],
                ( kwargs.variant )                    : ['snp_id', 'allele'],
            ],
            join: [
                drug_recommendation: [ "left join", "on (jpgdr.drug_recommendation_id = dr.id)" ],
                genotype_drug_recommendation: [ "left join", "using (drug_recommendation_id)" ],
                ( kwargs.genotype ): [ "left join", "using (job_id, patient_id, haplotype_name1, haplotype_name2, het_combo)" ],
                ( kwargs.geneHaplotype ): [ "left join", """\
                    on (
                        jpgh.job_id = jpg.job_id and
                        jpgh.patient_id = jpg.patient_id and
                        jpgh.gene_name = jpg.gene_name and
                        jpgh.haplotype_name = jpg.haplotype_name1 and
                        jpgh.het_combo = jpg.het_combo
                       ) or (
                        jpgh.job_id = jpg.job_id and
                        jpgh.patient_id = jpg.patient_id and
                        jpgh.gene_name = jpg.gene_name and
                        jpgh.haplotype_name = jpg.haplotype_name2 and
                        jpgh.het_combo = jpg.het_combo
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
            ],
            where: "jpgdr.job_id = :job_id",
            sqlParams: kwargs.sqlParams,
            duplicateKey: [
                /* Repeat drug recommendation details for different patients
                 */
                drug_recommendation : ['id', [ ( kwargs.genotypeDrugRecommendation ) : ['job_id', 'patient_id'] ]],
                /* Repeat genotypes for different drug recommendations
                 */
                ( kwargs.genotype ) : ['id', [ drug_recommendation : ['id'] ]],
                /* Don't repeat a haplotype for the same patient
                 */
                ( kwargs.geneHaplotype ) : ['job_id', 'patient_id', 'gene_name', 'haplotype_name'],
                /* Repeat variants for the same patient but a different haplotype (but we don't 
                 * repeat variants with the same allele and snp_id but only a different zygosity)
                 */
                ( kwargs.variant ): ['job_id', 'patient_id', [ ( kwargs.geneHaplotype ) : [ 'gene_name', 'haplotype_name' ] ], 'allele', 'snp_id'],
            ])
    }

    /** Generate a report, which is a Report.condensedJoin with keys of each row replaced with aliases.
     *
     * @param kwargs.fillWith a function of type ( row, column -> value ) that replaces missing or 
     * null values in the join (default: null).
     */
    private static def report(Map kwargs = [:], groovy.sql.Sql sql) {
        /* Replace {table}.{field} names from MySQL with user-friendlier aliases.
         */
        Map aliases = [
            PATIENT_ID      : 'SAMPLE_ID',
            GENE_NAME       : 'GENE',
            DRUG_NAME       : 'DRUG',
            PHENOTYPE_NAME  : 'PHENOTYPE',
            HAPLOTYPE_NAME1 : 'HAPLOTYPE1',
            HAPLOTYPE_NAME2 : 'HAPLOTYPE2',
            HAPLOTYPE_NAME  : 'HAPLOTYPE',
            SNP_ID          : 'RS#',
            HET_COMBO       : 'HET_COMBO',
            HET_COMBOS      : '#HET_COMBOS',
        ]
        Row.changeKeys(
            haplorec.util.sql.Report.condensedJoin(
                [
                    replace: { row, column -> row[column] == null },
                ] + kwargs, 
                sql
            ),
            to: { header, h ->
                // remove table prefix (e.g. job_patient_variant.snp_id => snp_id)
                def k = h.replaceAll(/^[^.]+\.(.*)/, { it[1] })
                         .toUpperCase()
                aliases.get(k, k)
            },
        )
    }

}
