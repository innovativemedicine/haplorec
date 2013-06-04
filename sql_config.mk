# will probably calculate these using sample datasets in the future (2 * max length seen)

MAXLEN_HEURISTIC = $(shell bc <<< "$(1)*2")
# NOTE: max key size is 3072 bytes (http://docs.oracle.com/cd/E17952_01/refman-5.5-en/innodb-restrictions.html)
VARCHAR_MAXLEN = 200 

# haplotype
export SQL_HAPLOTYPE_NAME_LENGTH = $(VARCHAR_MAXLEN)
export SQL_GENE_NAME_LENGTH = $(VARCHAR_MAXLEN)
# gene_haplotype_variant 
export SQL_HAPLOTYPE_ID_LENGTH = $(VARCHAR_MAXLEN)
export SQL_SNP_ID_LENGTH = $(VARCHAR_MAXLEN)
export SQL_ALLELE_LENGTH = $(VARCHAR_MAXLEN)
# drug_recommendation
export SQL_DRUG_NAME_LENGTH = $(VARCHAR_MAXLEN)
# phenotype
export SQL_PHENOTYPE_NAME_LENGTH = $(call MAXLEN_HEURISTIC,143)
# job 
export SQL_JOB_NAME_LENGTH = $(VARCHAR_MAXLEN)

# job tables

export SQL_PATIENT_ID_LENGTH = $(VARCHAR_MAXLEN)
export SQL_PHYSICAL_CHROMOSOME_LENGTH = $(VARCHAR_MAXLEN)

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
