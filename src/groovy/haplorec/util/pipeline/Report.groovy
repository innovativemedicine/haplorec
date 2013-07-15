package haplorec.util.pipeline

import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import groovy.sql.GroovyRowResult
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.ResultSetMetaData

import haplorec.util.Row
// import haplorec.util.sql.Report (we use this, but we can't import it due to conflicting names)

public class Report {

    /* Given a job_id, return an iterator over GeneHaplotypeMatrix's, 1 for each distinct gene identified in 
     * uniqueHaplotype (for this job).
     */
    static def novelHaplotypeReport(Map kwargs = [:], groovy.sql.Sql sql) {
        kwargs += Pipeline.tables(kwargs)
        return new Object() {
            def each(Closure f) {
				withConnection(sql) { connection ->
	                def patientVariantsStmt = stmtIter(connection, """\
	                        |select snp_id, allele, patient_id, physical_chromosome
	                        |from ${kwargs.uniqueHaplotype}
	                        |join ${kwargs.variant} using (job_id, patient_id, physical_chromosome)
	                        |where job_id = ? and gene_name = ?
	                        |order by job_id, gene_name, patient_id, physical_chromosome, snp_id
	                        |""".stripMargin())
	                def haplotypeVariantsStmt = stmtIter(connection, """
	                        |select haplotype_name, snp_id, allele
	                        |from gene_haplotype_variant
	                        |where gene_name = ?
	                        |order by gene_name, haplotype_name, snp_id
	                        |""".stripMargin())
	                sql.eachRow("select distinct gene_name from ${kwargs.uniqueHaplotype} where job_id = :job_id order by gene_name".toString(), kwargs.sqlParams) { row ->
	                    def snpIds = sql.rows("select snp_id from gene_snp where gene_name = :gene_name order by snp_id", row).collect { it.snp_id }
	                    patientVariantsStmt.execute(kwargs.sqlParams.job_id, row.gene_name)
	                    haplotypeVariantsStmt.execute(row.gene_name)
	                    f(new GeneHaplotypeMatrix(geneName: row.gene_name, snpIds: snpIds, patientVariants: patientVariantsStmt, haplotypeVariants: haplotypeVariantsStmt))
	                }
				}
            }
        }
    }

    static def phenotypeDrugRecommendationReport(Map kwargs = [:], groovy.sql.Sql sql) {
        kwargs += Pipeline.tables(kwargs)
        report(sql,
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
            ],
            where: "jppdr.job_id = :job_id",
            sqlParams: kwargs.sqlParams,
            duplicateKey: [
                // repeat drug recommendation details for different patients
                drug_recommendation : ['id', [ ( kwargs.phenotypeDrugRecommendation ) : ['job_id', 'patient_id'] ]],
                // repeat phenotypes for different drug recommendations
                ( kwargs.genePhenotype ) : ['id', [ drug_recommendation : ['id'] ]],
                // don't repeat a haplotype for the same patient
                ( kwargs.geneHaplotype ) : ['job_id', 'patient_id', 'gene_name', 'haplotype_name'],
                // repeat variants for the same patient but a different haplotype (but we don't 
                // repeat variants with the same allele and snp_id but only a different zygosity)
                ( kwargs.variant )       : ['job_id', 'patient_id', [ ( kwargs.geneHaplotype ) : [ 'gene_name', 'haplotype_name' ] ], 'allele', 'snp_id'],
            ])
    }

    static def genotypeDrugRecommendationReport(Map kwargs = [:], groovy.sql.Sql sql) {
        kwargs += Pipeline.tables(kwargs)
        report(sql,
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
            ],
            where: "jpgdr.job_id = :job_id",
            sqlParams: kwargs.sqlParams,
            duplicateKey: [
                // repeat drug recommendation details for different patients
                drug_recommendation : ['id', [ ( kwargs.genotypeDrugRecommendation ) : ['job_id', 'patient_id'] ]],
                // repeat genotypes for different drug recommendations
                ( kwargs.genotype ) : ['id', [ drug_recommendation : ['id'] ]],
                // don't repeat a haplotype for the same patient
                ( kwargs.geneHaplotype ) : ['job_id', 'patient_id', 'gene_name', 'haplotype_name'],
                // repeat variants for the same patient but a different haplotype (but we don't 
                // repeat variants with the same allele and snp_id but only a different zygosity)
                ( kwargs.variant )       : ['job_id', 'patient_id', [ ( kwargs.geneHaplotype ) : [ 'gene_name', 'haplotype_name' ] ], 'allele', 'snp_id'],
            ])
    }

    private static def report(Map kwargs = [:], groovy.sql.Sql sql) {
        Map aliases = [
            PATIENT_ID      : 'SAMPLE_ID',
            GENE_NAME       : 'GENE',
            DRUG_NAME       : 'DRUG',
            PHENOTYPE_NAME  : 'PHENOTYPE',
            HAPLOTYPE_NAME1 : 'HAPLOTYPE1',
            HAPLOTYPE_NAME2 : 'HAPLOTYPE2',
            HAPLOTYPE_NAME  : 'HAPLOTYPE',
            SNP_ID          : 'RS#',
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

    private static def iterAsList(iter) {
        def xs = []
        iter.each { xs.add(it) }
        return xs
    }

    public static class GeneHaplotypeMatrix {

        // The gene_name that this haplotype matrix is for.
        def geneName
        // A list of snp_id's, representing the snps for this gene.
        List snpIds
        // An iterable over rows of snp_id, allele, patient_id, physical_chromosome,
        // ordered by those fields.
        def patientVariants
        // An iterable over rows of haplotype_name, snp_id, allele
        // ordered by those fields.
        def haplotypeVariants

        String toString() {
            def j = { xs -> xs.join(',' + String.format('%n')) }
            "GeneHaplotypeMatrix(${j([geneName, snpIds, '[' + j(iterAsList(patientVariants)) + ']', '[' + j(iterAsList(haplotypeVariants)) + ']'])})"
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
            Row.groupBy(patientVariants, ['patient_id', 'physical_chromosome']).each { variants ->
                def patientId = variants[0].patient_id
                f(
                    new NovelHaplotype(
                        patientId: variants[0].patient_id,
                        physicalChromosome: variants[0].physical_chromosome,
                    ),
                    alleles(variants),
                )
            }

        }

    }

    private static GroovyRowResult nextRow(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        ArrayList list = new ArrayList(50);
        HashMap row = new HashMap(columns);
        for(int i = 1; i <= columns; i++){           
            row.put(md.getColumnName(i), rs.getObject(i));
        }
        return new GroovyRowResult(row);
    }
	
	private static def withConnection(groovy.sql.Sql sql, Closure f) {
		if (sql.connection == null) {
			// This Sql instance was created from a DataSource.
			def connection = sql.dataSource.getConnection()
			try {
				f(connection)
			} finally {
                // Return the connection to the connection pool.
				connection.close()
			}
		} else {
			f(sql.connection)
		}
	}

    private static def stmtIter(connection, String sqlStr) {
        def stmt = connection.prepareStatement(sqlStr)
        ResultSet rs = null
        return new Object() {
            def execute(Object... params) {
                (1..params.size()).each { i ->
                    stmt.setObject(i, params[i-1])
                }
                rs = stmt.executeQuery()
            }
            def each(Closure f) {
                if (rs == null) {
                    throw new IllegalStateException("Must call execute(params) before next()")
                }
                while (rs.next()) {
                    def row = nextRow(rs)
                    f(row)
                }
                rs.close()
            }
        }
    }

}
