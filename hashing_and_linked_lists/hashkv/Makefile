CXX ?= c++
CXXFLAGS ?= -O2 -std=c++17 -Wall -Wextra -pthread
CPPFLAGS += -Iinclude

YCSB_DIR ?= ../YCSB-cpp
BUILD_DIR := build
STORE_SRC := src/kv_store.cc src/wal.cc
STORE_HDR := include/hashkv/kv_store.h
WAL_HDR := src/wal.h
TEST_BIN := $(BUILD_DIR)/kv_store_test
WAL_TEST_BIN := $(BUILD_DIR)/wal_recovery_test
YCSB_BIN := $(BUILD_DIR)/ycsb_hashkv
PROFILE_BIN := $(BUILD_DIR)/ycsb_hashkv_profile
CHURN_BIN := $(BUILD_DIR)/churn_bench
SPACE_BIN := $(BUILD_DIR)/space_bench
YCSB_CORE := $(wildcard $(YCSB_DIR)/core/*.cc)

.PHONY: all test bench bench-all clean

all: $(TEST_BIN) $(YCSB_BIN)

$(BUILD_DIR):
	mkdir -p $@

$(TEST_BIN): $(STORE_SRC) tests/kv_store_test.cc $(STORE_HDR) $(WAL_HDR) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(filter %.cc,$^) -o $@

$(WAL_TEST_BIN): $(STORE_SRC) tests/wal_recovery_test.cc $(STORE_HDR) $(WAL_HDR) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -DHASHKV_TEST_CRASH $(filter %.cc,$^) -o $@

$(YCSB_BIN): $(STORE_SRC) src/ycsb_adapter.cc $(YCSB_CORE) $(STORE_HDR) $(WAL_HDR) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -I$(YCSB_DIR) $(filter %.cc,$^) -o $@

$(PROFILE_BIN): $(STORE_SRC) src/ycsb_adapter.cc $(YCSB_CORE) $(STORE_HDR) $(WAL_HDR) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -DHASHKV_PROFILE -I$(YCSB_DIR) $(filter %.cc,$^) -o $@

$(CHURN_BIN): $(STORE_SRC) tests/churn_bench.cc $(STORE_HDR) $(WAL_HDR) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(filter %.cc,$^) -o $@

$(SPACE_BIN): $(STORE_SRC) tests/space_bench.cc $(STORE_HDR) $(WAL_HDR) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(filter %.cc,$^) -o $@

test: $(TEST_BIN) $(WAL_TEST_BIN)
	./$(TEST_BIN)
	./$(WAL_TEST_BIN)

bench: $(YCSB_BIN)
	./bench.sh

bench-all: $(YCSB_BIN)
	./bench_all.sh

clean:
	rm -rf $(BUILD_DIR)
