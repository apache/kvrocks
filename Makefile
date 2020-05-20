# Top level makefile, the real shit is at src/Makefile

default: all

clone_dependencies := $(shell sh -c 'git submodule init && git submodule update')
.DEFAULT:
	cd src && $(MAKE) $@

install:
	cd src && $(MAKE) $@

.PHONY: install
