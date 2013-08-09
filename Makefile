# Type "make help" for information about targets.

SHELL := /bin/bash
SRC := src
PYTHONSRC := $(SRC)/python
export PYTHONPATH := $(PYTHONSRC):$(PYTHONPATH)
RENDER := $(PYTHONSRC)/render.py
export SCRIPT := script
MAKE_SCRIPTS := $(SCRIPT)/makefile

MAKE_CONFIG_FILES := $(shell find . -maxdepth 1 -name '*config.mk')

all: src/sql/mysql/haplorec.sql

include $(MAKE_CONFIG_FILES)

%: %.jinja $(MAKE_CONFIG_FILES)
	$(RENDER) $<

help:
	@$(MAKE_SCRIPTS)/usage.py
