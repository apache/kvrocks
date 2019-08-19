BUILD_DIR=./build
INSTALL_DIR=/usr/local
BIN_DIR=$(INSTALL_DIR)/bin
INSTALL=/usr/bin/install

all: kvrocks
.PHONY: all test

kvrocks:
	@mkdir -p $(BUILD_DIR)
	@sh ./build.sh $(BUILD_DIR)
	@echo "" 
	@echo "Hint: It's a good idea to run 'make test' ;)"
	@echo ""

test:
	@./$(BUILD_DIR)/unittest

clean:
	@rm -rf $(BUILD_DIR)/*

distclean:
	@rm -rf $(BUILD_DIR)/*

install:
	mkdir -p $(BIN_DIR)
	$(INSTALL) $(BUILD_DIR)/kvrocks $(BIN_DIR)
	$(INSTALL) $(BUILD_DIR)/kvrocks2redis $(BIN_DIR)
	@echo ""
	@echo "Installed success, everying is ok!"
	@echo ""
