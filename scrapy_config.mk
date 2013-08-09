CSV_OUTPUT_DIR_RELATIVE := tmp/scrapy
DB_FILES_DIR_RELATIVE := $(CSV_OUTPUT_DIR_RELATIVE)/mysql
export CSV_OUTPUT_DIR := $(abspath $(CSV_OUTPUT_DIR_RELATIVE))
export DB_FILES_DIR := $(abspath $(DB_FILES_DIR_RELATIVE))

CSV_FILENAMES := drug_recommendation.csv gene_haplotype_variant.csv genotype_drug_recommendation.csv genotype_phenotype.csv
CSV_FILES := $(addprefix $(CSV_OUTPUT_DIR_RELATIVE)/,$(CSV_FILENAMES))
DB_FILES := $(addprefix $(DB_FILES_DIR_RELATIVE)/,$(CSV_FILENAMES))

export HAPLOREC_DB_NAME := haplorec

$(CSV_OUTPUT_DIR_RELATIVE): 
	cd src/python/pharmgkb && scrapy crawl GeneDrugPair
	touch $(CSV_OUTPUT_DIR)

$(DB_FILES_DIR_RELATIVE)/%.csv: $(CSV_OUTPUT_DIR_RELATIVE)/%.csv
	mkdir -p $(dir $@)
	$(SCRIPT)/collapse_scraped_data.py $^ --output $@

dbfiles: $(DB_FILES)

load_haplorec: $(DB_FILES)
	$(SCRIPT)/load_dsv.py $(HAPLOREC_DB_NAME) \
		$(DB_FILES_DIR_RELATIVE)/drug_recommendation.csv \
		$(DB_FILES_DIR_RELATIVE)/gene_haplotype_variant.csv \
		$(DB_FILES_DIR_RELATIVE)/genotype_phenotype.csv \
		$(DB_FILES_DIR_RELATIVE)/genotype_drug_recommendation.csv \
		--map \
			"genotype_drug_recommendation: gene_name, haplotype_name1, haplotype_name2, drug_name => drug_recommendation" \
		--ignore \
			"genotype_drug_recommendation.drug_name" \
			\
			"drug_recommendation.gene_name" \
			"drug_recommendation.haplotype_name1" \
			"drug_recommendation.haplotype_name2" \
			\
			"genotype_phenotype.phenotype_genotype"

scrape: $(CSV_OUTPUT_DIR_RELATIVE)

.PHONY: dbfiles load_haplorec scrape
