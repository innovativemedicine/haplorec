# will probably calculate these using sample datasets in the future (2 * max length seen)

# haplotype
export SQL_HAPLOTYPE_NAME_LENGTH = 50
export SQL_GENE_NAME_LENGTH = 50
# haplotype_snps
export SQL_HAPLOTYPE_ID_LENGTH = 50
export SQL_SNP_ID_LENGTH = 50
export SQL_ALLELE_LENGTH = 50
# drug_recommendation
export SQL_DRUG_NAME_LENGTH = 50
export SQL_IMPLICATIONS_LENGTH = 50
export SQL_RECOMMENDATION_LENGTH = 50
export SQL_CLASSIFICATION_LENGTH = 50
export SQL_DIPLOTYPE_EGS_LENGTH = 50
export SQL_DIPLOTYPE_EGS_LENGH = 50
# phenotype
export SQL_PHENOTYPE_LENGTH = 50
export SQL_PHENOTYPE_NAME_LENGTH = 50
# pipeline_job
export SQL_JOB_NAME_LENGTH = 50

MYSQL_ENGINE_TYPE := InnoDB
CUBRID_ENGINE_TYPE :=
# one of: MySQL, CUBRID
DB_TYPE := MySQL

ifeq ($(DB_TYPE),MySQL)
	SQL_ENGINE_TYPE = $(MYSQL_ENGINE_TYPE)
else
ifeq ($(DB_TYPE),CUBRID)
	SQL_ENGINE_TYPE = $(CUBRID_ENGINE_TYPE)
endif
endif

ifeq ($(strip $(SQL_ENGINE_TYPE)),)
	SQL_ENGINE = 
else
	SQL_ENGINE = ENGINE=$(SQL_ENGINE_TYPE)
endif
export SQL_ENGINE
