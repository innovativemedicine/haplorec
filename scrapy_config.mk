CSV_OUTPUT_DIR_RELATIVE = tmp/scrapy
DB_FILES_DIR_RELATIVE = $(CSV_OUTPUT_DIR_RELATIVE)/mysql
export CSV_OUTPUT_DIR = $(abspath $(CSV_OUTPUT_DIR_RELATIVE))
export DB_FILES_DIR = $(abspath $(DB_FILES_DIR_RELATIVE))

CSV_FILENAMES := drug_recommendation.csv gene_haplotype_variant.csv genotype_drug_recommendation.csv genotype_phenotype.csv
CSV_FILES := $(addprefix $CSV_OUTPUT_DIR/,$(CSV_FILENAMES))
DB_FILES := $(addprefix $CSV_OUTPUT_DIR/,$(CSV_FILENAMES))

$(CSV_OUTPUT_DIR_RELATIVE): 
	cd src/python/pharmgkb && scrapy crawl GeneDrugPair
	touch $(CSV_OUTPUT_DIR)

derpy.txt: $(CSV_OUTPUT_DIR_RELATIVE)
	echo hi > derpy.txt
