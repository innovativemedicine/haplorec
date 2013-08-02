package haplorec.util.pipeline

import haplorec.util.Input
import haplorec.util.Row
import haplorec.util.Sql
import static haplorec.util.Sql._ as _
import haplorec.util.dependency.Dependency
import haplorec.util.data.GeneHaplotypeMatrix

import haplorec.util.pipeline.Algorithm

/**
 * Defines the stages of the haplorec pipeline, and wires up the stages into a dependency graph so 
 * that they can be run on input.
 *
 * Typical usage:
 * def (jobId, dependencies) = Pipeline.pipelineJob(sql)
 * Pipeline.buildAll(dependencies)
 */
public class Pipeline {
    /** Table alias to SQL table name mapping.
     * Aliases are used for brevity in SQL query construction.
     * Aliases are defined for the job_patient_* tables (just a camel-cased version with job_patient_ prefix removed).
     */
    private static Map defaultTables = [
        variant                     : 'job_patient_variant',
        hetVariant                  : 'job_patient_het_variant',
        genePhenotype               : 'job_patient_gene_phenotype',
        genotype                    : 'job_patient_genotype',
        geneHaplotype               : 'job_patient_gene_haplotype',
        novelHaplotype              : 'job_patient_novel_haplotype',
        genotypeDrugRecommendation  : 'job_patient_genotype_drug_recommendation',
        phenotypeDrugRecommendation : 'job_patient_phenotype_drug_recommendation',
        job                         : 'job',
    ]
    /** SQL table to table alias mapping (inverse mapping of defaultTables).
     */
    private static Map tableToAlias = defaultTables.inject([:]) { m, entry ->
        m[entry.value] = entry.key
        return m
    }
    /** Tables that are populated by the stages (i.e. all job_patient_* tables).
     */
    private static def stageTables = defaultTables.grep {
        it.value.startsWith('job_patient_')
    }.inject([:]) { m, entry -> 
        m[entry.key] = entry.value
        return m 
    }
    /** Aliases of tables that are affected by heterozygote combinations.
     * That is, tables with het_combo and het_combos fields to keep track of which combination of 
     * heterozygous variants from job_patient_het_variant this row results from.
     */
    private static Set hetComboTables = [
        'hetVariant',
        'genePhenotype',
        'genotype',
        'geneHaplotype',
        'novelHaplotype',
        'genotypeDrugRecommendation',
        'phenotypeDrugRecommendation',
    ] as Set

    /** Pipeline Stage Definitions.
     * =============================================================================================
     * Each stage is named like {source}To{Target}, where source is the input table alias and target 
     * is the output table alias. 
     *
     * All stages take the same arguments:
     * @param sql a connection to the haplorec database
     * @param kwargs.sqlParams.job_id the job_id to run this stage for
     * @param kwargs.{tableAlias} the SQL table to use for tableAlias
     * @param kwargs.meta metadata about the columns for a given table alias.  For example
     * kwargs.meta == [
     *     variant: [
     *         columns: [id, job_id, patient_id, physical_chromosome, snp_id, allele, zygosity], 
     *         primaryKey: [id],
     *     ],
     *     ...
     * ]
     */

    /** Add default keyword arguments to kwargs used by the stages.
     * This adds things like defaultTables.
     */
    private static def setDefaultKwargs(Map kwargs) {
        def setDefault = { property, defaultValue -> 
            /* Only add property to kwargs if it's not already defined.
             */
            if (!kwargs.containsKey(property)) {
                kwargs[property] = defaultValue
            }
        }
        setDefault('saveAs', 'existing')
        defaultTables.keySet().each { tableAlias ->
            setDefault(tableAlias, defaultTables[tableAlias]) 
        }
    }

    /** Populate genotype from geneHaplotype by grouping pairs of haplotype_name's with the same 
     * patient_id/gene_name/het_combo into haplotype_name1, haplotype_name2.
     * Pairs are sorted (i.e. [*1, *2] not [*2, *1]).
     * If only one haplotype was detected, hapltoype_name1 is filled and haplotype_name2 is null.
     */
    private static def geneHaplotypeToGenotype(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)
        def groupBy = ['job_id', 'patient_id', 'gene_name', 'het_combo']
		Sql.groupedRowsToColumns(sql, kwargs.geneHaplotype, kwargs.genotype,
			groupBy, 
            [
            /* Copy the rows that geneHaplotype and genotype have in common as is.
             */
            'job_id'         : 'job_id',
            'patient_id'     : 'patient_id',
            'gene_name'      : 'gene_name',
            'het_combo'      : 'het_combo',
            'het_combos'     : 'het_combos',
            /* Given a pair of geneHaplotype rows, copy the first haplotype_name into 
             * haplotype_name1 and the second into haplotype_name2.
             */
            'haplotype_name' : ['haplotype_name1', 'haplotype_name2']
            ],
            /* haplotype_name1 < haplotype_name2
             */
			orderRowsBy: ['haplotype_name'],
			sqlParams: kwargs.sqlParams,
			rowTableWhere: "${kwargs.geneHaplotype}.job_id = :job_id",
		)
    }

    /** Populate phenotypeDrugRecommendation from genePhenotype by inserting all 
     * DrugRecommendation's where the mapping:
     * { (GeneName, PhenotypeName) } -> DrugRecommendation 
     * defined by gene_phenotype_drug_recommendation is satisfied.
     */
    static def genePhenotypeToPhenotypeDrugRecommendation(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)
        return Sql.selectWhereSetContains(
            sql,
            /* { (GeneName, PhenotypeName) } -> DrugRecommendation 
             */
            'gene_phenotype_drug_recommendation',
            /* Job, Patient, { (GeneName, PhenotypeName) }
             */
            kwargs.genePhenotype,
            ['gene_name', 'phenotype_name'],
            tableAGroupBy: ['drug_recommendation_id'],
            tableBGroupBy: ['job_id', 'patient_id', 'het_combo', 'het_combos'],
            /* Job, Patient, DrugRecommendation
             */
            select: jobPatientColumns(kwargs.meta, 'phenotypeDrugRecommendation'),
            intoTable: kwargs.phenotypeDrugRecommendation,
            saveAs: kwargs.saveAs, 
            sqlParams: kwargs.sqlParams,
            tableBWhere: { t -> "${t}.job_id = :job_id" },
        )
    }

    private static String _eq(sqlParams) {
        /* _eq([gene_name: 'g1', job_id: 1]) // "gene_name = :gene_name and job_id = :job_id"
         */
        sqlParams.collect { param -> "${param.key} = :${param.key}" }.join(" and ") 
    }

    /** Populate geneHaplotype and novelHaplotype from variant.
     *
     * Given a list of variants (belonging to the same chromosome), we call a haplotype if there is 
     * a subset variants that uniquely identifies a known haplotype.
     *
     * Novel haplotypes are called for a gene if:
     * 1. There's at least one input variant whose SNP ID belongs to that gene, but whose allele is 
     *    not from any known haplotype.
     * 2. All the input variants exist in known haplotypes for that gene (and there's at least one), 
     *    but no known haplotype has this particular variant combination.
     *
     * Pseudocode:
     *
     * for each gene in "genes with at least one patient having at least one snp_id for this gene":
     *     ghm = GeneHaplotypeMatrix(gene)
     *     for each patient in "patients having at least one snp_id for this gene":
     *         for physical_chromosome in ['A', 'B']:
     *             homs = "hom variants for this patient, belonging to snp_ids for this gene, on this physical_chromosome"
     *             int het_combos = "select distinct het_combos from hetVariant for this patient and gene"
     *             for het_combo = 1..het_combos:
     *                 hets = "het variants for this patient, gene, and het_combo"
     *                 Set haplotypes = ghm.variantsToHaplotypes(homs + hets)
     *                 if len(haplotypes) == 1:
     *                     insert haplotypes[0] into geneHaplotype
     *                 elif len(haplotypes) == 0:
     *                     insert gene into novelHaplotype
     *                 else:
     *                     skip ambiguous haplotypes
     */
    static def variantToGeneHaplotypeAndNovelHaplotype(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)

        def distinctGeneAndPatientSql = { Map kws = [:], variantsTable -> 
            /* Select all (gene_name, patient_id) tuples where there exists at least one variant for 
             * that gene for that patient.
             *
             * Note that "allele is not null" is used to filter out lines from the variant 
             * file input where a SNP is listed, but no allele is present.
             */
            """\
            |select distinct gene_name, patient_id
            |from ${variantsTable}
            |join gene_haplotype_variant using (snp_id)
            |where ${variantsTable}.allele is not null and job_id = :job_id 
            |${_(kws.where, return: { where -> "and $where" })}
            |""".stripMargin()
        }
        /* GeneName -> { PatientID }
         */
        def geneToPatientId = tuplesToMapOfSets(
            Row.map(
                Sql.selectAs(sql, """\
                    |${distinctGeneAndPatientSql(kwargs.variant, where: "zygosity != 'het'")}
                    |union distinct
                    |${distinctGeneAndPatientSql(kwargs.hetVariant)}
                    |""".stripMargin(), null,
                    sqlParams: kwargs.sqlParams,
                    saveAs: 'rows')
            ) { row -> 
                [row.gene_name, row.patient_id] 
            }
        )

        geneToPatientId.keySet().each { geneName ->
            GeneHaplotypeMatrix ghm = GeneHaplotypeMatrix.haplotypeMatrix(sql, geneName)
            List geneHaplotypeRows = []
            List novelHaplotypeRows = []
            geneToPatientId[geneName].each { patientId ->
                def sqlParams = kwargs.sqlParams + [gene_name: geneName, patient_id: patientId]
                /* physical_chromosome -> [homozygous variants]
                 */
                def homVars = Sql.selectAs(sql, """\
                    |select *
                    |from ${kwargs.variant}
                    |join gene_snp using (snp_id)
                    |where zygosity = 'hom' and ${_eq(sqlParams)}
                    |order by physical_chromosome
                    |""".stripMargin(), null,
                    sqlParams: sqlParams,
                    saveAs: 'rows')
                    .groupBy { it.physical_chromosome }
                /* physical_chromosome -> ( het_combo -> [heterozygous variants] )
                 */
                def hetVariantCombos = Sql.selectAs(sql, """\
                    |select *
                    |from ${kwargs.hetVariant}
                    |join gene_snp using (snp_id)
                    |where ${_eq(sqlParams)}
                    |order by het_combo, physical_chromosome
                    |""".stripMargin(), null,
                    sqlParams: sqlParams,
                    saveAs: 'rows')
                    .inject([:]) { m, row ->
                        m.get(row.physical_chromosome, [:])
                         .get(row.het_combo, [])
                         .add(row)
                        return m
                    }
                ['A', 'B'].each { physicalChromosome -> 
                    def homVariants = homVars.get(physicalChromosome, [])
                    (
                        hetVariantCombos[physicalChromosome] ?: 
                        /* There are only homozygous variants; no heterozygous variants.  
                         * Just use het_combo == het_combos == 1 and an empty list for hetVariants.
                         */
                        [ ( 1 ) : [] ]
                    ).each { hetCombo, hetVariants -> 
                        def hetCombos = hetVariantCombos[physicalChromosome] != null ? hetVariants[0].het_combos : 1
                        /* Find the haplotypes on a particular physical chromosome of a particular 
                         * heterozygous combination.
                         */
                        Set haplotypes = ghm.variantsToHaplotypes(homVariants + hetVariants)
                        if (haplotypes.size() == 1) {
                            /* An unambiguous, known haplotype.
                             */
                            geneHaplotypeRows.add([
                                job_id: sqlParams.job_id,
                                patient_id: patientId,
                                physical_chromosome: physicalChromosome,
                                het_combo: hetCombo,
                                het_combos: hetCombos,
                                gene_name: geneName,
                                haplotype_name: haplotypes.iterator().next(),
                            ])
                        } else if (haplotypes.size() == 0) {
                            /* A novel haplotype.
                             */
                            novelHaplotypeRows.add([
                                job_id: sqlParams.job_id,
                                patient_id: patientId,
                                physical_chromosome: physicalChromosome,
                                het_combo: hetCombo,
                                het_combos: hetCombos,
                                gene_name: geneName,
                            ])
                        } else {
                            /* haplotypes.size() > 0; variants ambiguously identify many haplotypes. 
                             * Default behaviour (for now) is to ignore these.
                             */
                        }
                    }
                }
            }
            /* Insert rows generated from the final geneName.
            */
            Sql.insert(sql, kwargs.geneHaplotype, null, geneHaplotypeRows)
            Sql.insert(sql, kwargs.novelHaplotype, null, novelHaplotypeRows)
        }

    }

    /** Populate hetVariant from variant by running Algorithm.disambiguateHets for each patient and 
     * their 'het' variants (for whichever gene their variants belong to).
     *
     * Pseudocode:
     *
     * for each gene in "genes with at least one patient having at least one heterozygous variant with a snp_id for this gene":
     *     ghm = GeneHaplotypeMatrix(gene)
     *     for each patient in "patients having at least one heterozygous variant with a snp_id for this gene":
     *         ambiguous_hets = "heterozygous variants for this patient belonging to snp_ids for this gene"
     *         het_combo = 1
     *         possible_het_combos = disambiguateHets(ghm, ambiguous_hets)
     *         int het_combos = sum [len(c) for c in combos for het_combo_type, combos in possible_het_combos]
     *         for (het_combo_type, combos) in possible_het_combos:
     *             # het_combo_type is one of:
     *             # AKnownBKnown => hets make up 2 known haplotypes for this gene
     *             # AKnownBNovel => hets make up 1 known haplotype and 1 novel haplotype for this gene
     *             for hets in combos:
     *                 for h in hets:
     *                     insert into hetVariant 
     *                         h.snp_id, h.allele, h.physical_chromosome, het_combo, het_combos
     *                 het_combo += 1
     */
    static def variantToHetVariant(Map kwargs = [:], groovy.sql.Sql sql) {

        /* GeneName -> { PatientID }
         */
        def geneToPatientId = tuplesToMapOfSets(
            Row.map(
                Sql.selectAs(sql, """\
                    |select distinct gene_name, patient_id
                    |from ${kwargs.variant}
                    |join gene_haplotype_variant using (snp_id)
                    |where zygosity = 'het' and job_id = :job_id
                    |""".stripMargin(), null,
                    sqlParams: kwargs.sqlParams,
                    saveAs: 'rows')
            ) { row -> 
                [row.gene_name, row.patient_id] 
            }
        )

        geneToPatientId.keySet().each { geneName ->
            GeneHaplotypeMatrix ghm = GeneHaplotypeMatrix.haplotypeMatrix(sql, geneName)
            geneToPatientId[geneName].each { patientId ->
                def sqlParams = kwargs.sqlParams + [gene_name: geneName, patient_id: patientId]
                def combos = Algorithm.disambiguateHets(
                    ghm, 
                    Sql.selectAs(sql, """\
                        |select snp_id, allele
                        |from ${kwargs.variant}
                        |join gene_snp using (snp_id)
                        |where zygosity = 'het' and ${_eq(sqlParams)}
                        |""".stripMargin(), null,
                        sqlParams: sqlParams,
                        saveAs: 'rows')
                )
                def columns
                int hetCombo = 1
                int hetCombos = combos.values().sum { combosOfHets -> combosOfHets.size() }
                combos.each { hetComboType, combosOfHets ->
                    /* Add het_combo and het_combos to each heterzygote variant before insertion 
                     * into hetVariant (for each possible combination).
                     */
                    combosOfHets.each { hets -> 
                        hets.each { h ->
                            h.het_combo = hetCombo
                            h.het_combos = hetCombos
                            h.job_id = kwargs.sqlParams.job_id
                            h.patient_id = patientId
							if (columns == null) {
								columns = h.keySet()
							}
                        }
                        hetCombo += 1
                    }
                }
				if (columns != null) {
					/* There's at least one hetVariant to insert.
					 */
					Sql.insert(sql, kwargs.hetVariant, columns, Row.flatten(Row.flatten(combos.values())))
				}
            }
        }

    }

    /** Given an iterable of tuples like [[x, y1], [x, y2]], return a map like [x: { y1, y2 }].
     */
    private static def tuplesToMapOfSets(tuples) {
        Row.inject(tuples, [:]) { m, pair ->
            def (x, y) = pair
            m.get(x, [] as Set).add(y)
            return m
        }
    }

    /** Populate genotypeDrugRecommendation from genotype by inserting all 
     * DrugRecommendation's where the mapping:
     * { (GeneName, HaplotypeName, HaplotypeName) } -> DrugRecommendation
     * defined by genotype_drug_recommendation is satisfied.
     */
    static def genotypeToGenotypeDrugRecommendation(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)
        return Sql.selectWhereSetContains(
            sql,
            /* { (GeneName, HaplotypeName, HaplotypeName) } -> DrugRecommendation
             */
            'genotype_drug_recommendation',
            /* Job, Patient, { (GeneName, HaplotypeName, HaplotypeName) }
             */
            kwargs.genotype,
            ['gene_name', 'haplotype_name1', 'haplotype_name2'],
            tableAGroupBy: ['drug_recommendation_id'],
            tableBGroupBy: ['job_id', 'patient_id', 'het_combo', 'het_combos'],
            /* Job, Patient, DrugRecommendation
             */
            select: jobPatientColumns(kwargs.meta, 'genotypeDrugRecommendation'),
            intoTable: kwargs.genotypeDrugRecommendation,
            saveAs: kwargs.saveAs, 
            sqlParams: kwargs.sqlParams,
			tableBWhere: { t -> "${t}.job_id = :job_id" },
        )
    }

    /** Populate genePhenotype from genotype by inserting all PhenotypeName's where the mapping:
     * (GeneName, HaplotypeName, HaplotypeName) -> (GeneName, PhenotypeName)
     * defined by genotype_phenotype is satisfied.
     */
	static def genotypeToGenePhenotype(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)
		return Sql.selectAs(sql, """\
			|select ${jobPatientColumns(kwargs.meta, 'genePhenotype').join(', ')} 
            |from ${kwargs.genotype} 
			|join genotype_phenotype using (gene_name, haplotype_name1, haplotype_name2)
			|where ${kwargs.genotype}.job_id = :job_id
            |""".stripMargin(),
			jobPatientColumns(kwargs.meta, 'genePhenotype'),
			saveAs:kwargs.saveAs,
			intoTable:kwargs.genePhenotype,
            sqlParams:kwargs.sqlParams,
		)
	}

    /** Convenience method; all the table's column names (except the id column).
     */
    private static def jobPatientColumns(Map meta, tableAlias) {
        meta[tableAlias].columns.grep { it != 'id' }
    }

    /** Construct the haplorec dependency graph, minus the rules for actually building the 
     * dependencies.
     * This is useful for extracting information about the graph (e.g. given a target, what are its 
     * dependencies).
     *
     * Returns a tuple of:
     * tbl: Table alias to SQL table name mapping (see defaultTables)
     * dependencies: a Map from table aliases to haplorec.util.pipeline.Dependency's
     */
    static def dependencyGraph(Map kwargs = [:]) {
        def tbl = tables()

        def builder = new PipelineDependencyGraphBuilder()
        Map dependencies = [:]
        def canUpload = { d -> PipelineInput.inputTables.contains(d) }

        dependencies.genotypeDrugRecommendation = builder.dependency(id: 'genotypeDrugRecommendation', target: 'genotypeDrugRecommendation', 
        name: "Genotype Drug Recommendations",
        table: tbl.genotypeDrugRecommendation,
        fileUpload: canUpload('genotypeDrugRecommendation')) {
            dependencies.genotype = dependency(id: 'genotype', target: 'genotype', 
            name: "Genotypes",
            table: tbl.genotype,
            fileUpload: canUpload('genotype')) {
                dependencies.geneHaplotype = dependency(id: 'geneHaplotype', target: 'geneHaplotype', 
                name: "Haplotypes",
                table: tbl.geneHaplotype,
                fileUpload: canUpload('geneHaplotype')) {
                    dependencies.hetVariant = dependency(id: 'hetVariant', target: 'hetVariant', 
                    name: "Heterozygous Variant Combinations",
                    table: tbl.hetVariant,
                    fileUpload: canUpload('hetVariant')) {
                        dependencies.variant = dependency(id: 'variant', target: 'variant', 
                        name: "Variants",
                        table: tbl.variant,
                        fileUpload: canUpload('variant'))
                    }
                }
            }
        }
        dependencies.phenotypeDrugRecommendation = builder.dependency(id: 'phenotypeDrugRecommendation', target: 'phenotypeDrugRecommendation', 
        name: "Phenotype Drug Recommendations",
        table: tbl.phenotypeDrugRecommendation,
        fileUpload: canUpload('phenotypeDrugRecommendation')) {
            dependencies.genePhenotype = dependency(id: 'genePhenotype', target: 'genePhenotype', 
            name: "Phenotypes",
            table: tbl.genePhenotype,
            fileUpload: canUpload('genePhenotype')) {
                dependency(refId: 'genotype')
            }
        }
        dependencies.novelHaplotype = builder.dependency(id: 'novelHaplotype', target: 'novelHaplotype', 
        name: "Novel Haplotypes",
        table: tbl.novelHaplotype,
        fileUpload: canUpload('novelHaplotype')) {
            dependency(refId: 'geneHaplotype')
        }

        return [tbl, dependencies]
    }

    /** Return a table alias to SQL table name mapping.
     */
    static def tables() {
		def tbl = new LinkedHashMap(defaultTables)
        return tbl
    }

    /** Perform the setup needed to run a pipeline job, then return a dependency graph (see 
     * dependencyGraph) with all the rules needed to build the targets filled in, but don't run any 
     * stages yet.
     *
     * Setup involves:
     * 1) Either:
     * - insert a new record into the job table
     * Or:
     * - delete the results of a previously run job if kwargs.jobId was provided (to ready job to 
     *   be re-run).
     *
     * @param sql a connection to the haplorec database
     * And:
     * @param kwargs.jobId use this job_id in all the populated tables instead of 
     * Or
     * @param kwargs.jobName create a new job with this job_name 
     */
    static def pipelineJob(Map kwargs = [:], groovy.sql.Sql sql) {
		def tableKey = { defaultTable ->
			defaultTable.replaceFirst(/^job_/,  "")
						.replaceAll(/_(\w)/, { it[0][1].toUpperCase() })
		}

        def (tbl, dependencies) = dependencyGraph(kwargs)

        if (kwargs.jobId == null) {
            /* Create a new job.
             */
            def keys = sql.executeInsert("insert into job(job_name) values(:jobName)", kwargs)
            kwargs.jobId = keys[0][0]
        } else {
            /* Given an existing jobId, delete all job_* rows, then rerun the pipeline.
             */
            if ((sql.rows("select count(*) as count from ${tbl.job} where job_id = :jobId".toString(), kwargs))[0]['count'] == 0) {
                throw new IllegalArgumentException("No such job with job_id ${kwargs.jobId}")
            }
            stageTables.each { __, jobTable ->
                sql.execute "delete from $jobTable where job_id = :jobId".toString(), kwargs
            }
        }

        /* Given a table alias and "raw" input (that is, in the sense that it may need to be 
         * filtered or error checked), build a SQL table from that input by inserting it with a new 
         * jobId.
         */
        def jobTableInsertColumns = stageTables.keySet().inject([:]) { m, alias ->
            def table = tbl[alias]
            m[alias] = Sql.tableColumns(sql, tbl[alias], where: "column_key != 'PRI'")
			return m
        }
        /* Given a table alias and raw input (i.e. any input argument accepted by pipelineInput), 
         * insert the validated input into its associated table.
         */
        def buildFromInput = { alias, rawInput ->
            def input = pipelineInput(alias, rawInput)
            def jobRowIter = new Object() {
                def each(Closure f) {
                    /* The set of fields we will add here, that are not already provided in input.
                    */
                    Set fieldsToAdd = ['job_id', 'het_combo', 'het_combos'] as Set
                    input.each { row ->
                        Map mapRow = [job_id: kwargs.jobId]
                        [
                            jobTableInsertColumns[alias].grep { !(it in fieldsToAdd) }, 
                            row
                        ].transpose().inject(mapRow) { m, kv ->
                            def (column, value) = kv
                            m[column] = value
                            return m
                        }
                        if (alias in hetComboTables) {
                            /* If we are given a stage in the pipeline as input, assume that there is only 1 
                             * heterozygous combination.
                             */
                            mapRow.het_combo = 1
                            mapRow.het_combos = 1
                        }
						f(mapRow)
                    }
                }
            }
            Sql.insert(sql, tbl[alias], null, jobRowIter)
        }
        /* Setup the kwargs for all the pipeline stages (refer to Pipeline Stage Definitions).
         */
        def stageKwargs = tbl + [
            sqlParams: [
                job_id: kwargs.jobId,
            ],
            meta: Sql.tblColumns(sql).inject([:]) { m, entry ->
                def (table, meta) = [entry.key, entry.value]
                if (tableToAlias.containsKey(table)) {
                    /* If this test fails, it might be a table not needed for the pipeline 
                     * input/output (e.g. job_status in haplorec-wui).
                     */
                    m[tableToAlias[table]] = meta
                }
                return m
            },
        ]

        /* Add rules for building each target in the dependency graph (using whatever inputs were 
         * specified in the arguments of Pipeline.pipelineJob).
         */
        dependencies.genotypeDrugRecommendation.rule = { ->
            genotypeToGenotypeDrugRecommendation(stageKwargs, sql)
        }
        dependencies.phenotypeDrugRecommendation.rule = { ->
            genePhenotypeToPhenotypeDrugRecommendation(stageKwargs, sql)
        }
        dependencies.genotype.rule = { ->
            geneHaplotypeToGenotype(stageKwargs, sql)
        }
        dependencies.geneHaplotype.rule = { ->
            variantToGeneHaplotypeAndNovelHaplotype(stageKwargs, sql)
        }
        dependencies.hetVariant.rule = { ->
            variantToHetVariant(stageKwargs, sql)
        }
        dependencies.novelHaplotype.rule = { ->
            /* Do nothing, since it's already been done in variantToGeneHaplotypeAndNovelHaplotype.
            */
        }
        dependencies.variant.rule = { ->
            if (kwargs.containsKey('variants')) {
                buildFromInput('variant', kwargs.variants)
            }
        }
        dependencies.genePhenotype.rule = { ->
            genotypeToGenePhenotype(stageKwargs, sql)
        }

        /* For datasets that are already provided, replace their rules with ones that insert their 
         * rows into the approriate job_* table.
         */
        dependencies.keySet().each { table ->
            if (table != 'variant') {
                def inputKey = table + 's'
                if (table in stageTables && kwargs[inputKey] != null) {
                    def input = kwargs[inputKey]
                    dependencies[table].rule = { ->
                        buildFromInput(table, input)
                    }
                }
            }
        }

        return [kwargs.jobId, dependencies]
    }
	
    /** Perform the setup needed to run a pipeline job, then build all the targets in the graph.
     * Returns the job_id of the job.
     */
	static def runJob(Map kwargs = [:], groovy.sql.Sql sql) {
        def (jobId, job) = pipelineJob(kwargs, sql)
        buildAll(job)
        return jobId
	}

    /** Given a pipeline job, build all the targets in the graph.
     */
    static def buildAll(Map<CharSequence, Dependency> job) {
        Set<Dependency> built = []
        buildAll(job, built)
    }

    /** Given a pipeline job, build all the targets in the graph, unless they've already been built, 
     * as indicated by their presense in built. 
     *
     * built will be modified to contain all built targets. 
     */
    static def buildAll(Map<CharSequence, Dependency> job, Set<Dependency> built) {
        Set<Dependency> targetsWithoutDependants = Dependency.dependants(job.values()).grep { entry ->
            def (dependency, dependants) = [entry.key, entry.value]
            /* Filter for the "end points" of the dependency graph.
            */
            dependants.size() == 0
        }.collect { it.key }
        targetsWithoutDependants.each { dependency ->
            dependency.build(built)
        }
    }
	
    /** Return an iterator over validated pipeline input if the input is from a stream or
     * filehandle, or just return the input as-is.
     *
     * Input validation is handled in PipelineInput, but validation only occurs when input is from a 
     * stream or a filehandle.
     *
     * @param tableAlias a table alias that can be used to determine which function to use from 
     * PipelineInput to process the input to extract the fields we're actually going to store in the 
     * database (e.g.  if tableAlias == 'variant', we'd return PipelineInput.variants(input)).
     * @param input 
     * input must conform to:
     * an iterable (or List) of Lists l whose values l[i] correspond to the i-th declared column of 
     * src/sql/mysql/haplorec.sql (minus 'job_id', and any other field not provided by the input 
     * source).
     * 
     * Special cases (these are cases in which tableAlias is used):
     * - if input is a String, treat it as a filepath and process it using a function from 
     *   PipelineInput
     * - if input a list of open filehandles, concatenate the result of processing each filehandle 
     *   using a function from PipelineInput
     */
    private static def pipelineInput(tableAlias, input) {
        if (input instanceof List && input.size() > 0 && input[0] instanceof BufferedReader) {
			/* Input is a list of filehandles (e.g. from a form submission, or open files on this machine).
             */
			def tableReader = PipelineInput.tableAliasToTableReader(tableAlias)
            return new Object() {
                def each(Closure f) {
                    input.each { inputStream ->
                        tableReader(inputStream).each { row ->
                            f(row)
                        }
                    }
                }
            }
        } else if (input instanceof Collection) {
            /* Input is a list of rows conforming to @param input specification.
             */
            return input
        } else if (input instanceof CharSequence) {
            /* Input is a filepath.
             */
            def tableReader = PipelineInput.tableAliasToTableReader(tableAlias)
            return tableReader(input)
        } else {
            /* Input is an iterable (i.e. defines .each) conforming to @param input specification.
             */
			return input
        }
    }

}
