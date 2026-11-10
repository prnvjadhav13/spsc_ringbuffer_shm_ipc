CXX ?= g++
BUILD_DIR ?= build

COMMON_SOURCES := shm_log_queue_mapping.cpp shm_queue_eventfd.cpp
HEADERS := $(wildcard *.hpp)
THREAD_FLAGS := -pthread
COMMON_WARNINGS := -Wall -Wextra -Wpedantic

PRODUCTION_DIR := $(BUILD_DIR)/release
DEBUG_DIR := $(BUILD_DIR)/debug

PRODUCTION_FLAGS := -std=c++23 $(COMMON_WARNINGS) -O3 -DNDEBUG $(THREAD_FLAGS)
DEBUG_FLAGS := -std=c++23 $(COMMON_WARNINGS) -O1 -g3 \
	-fsanitize=address,undefined -fno-omit-frame-pointer $(THREAD_FLAGS)

.PHONY: all release debug clean

all: release debug

release: $(PRODUCTION_DIR)/producer $(PRODUCTION_DIR)/consumer

debug: $(DEBUG_DIR)/producer $(DEBUG_DIR)/consumer

$(PRODUCTION_DIR) $(DEBUG_DIR):
	mkdir -p $@

$(PRODUCTION_DIR)/producer: shm_Prod_QueueOps.cpp $(COMMON_SOURCES) $(HEADERS) | $(PRODUCTION_DIR)
	$(CXX) $(CPPFLAGS) $(PRODUCTION_FLAGS) shm_Prod_QueueOps.cpp $(COMMON_SOURCES) -o $@

$(PRODUCTION_DIR)/consumer: shm_Consum_QueueOps.cpp $(COMMON_SOURCES) $(HEADERS) | $(PRODUCTION_DIR)
	$(CXX) $(CPPFLAGS) $(PRODUCTION_FLAGS) shm_Consum_QueueOps.cpp $(COMMON_SOURCES) -o $@

$(DEBUG_DIR)/producer: shm_Prod_QueueOps.cpp $(COMMON_SOURCES) $(HEADERS) | $(DEBUG_DIR)
	$(CXX) $(CPPFLAGS) $(DEBUG_FLAGS) shm_Prod_QueueOps.cpp $(COMMON_SOURCES) -o $@

$(DEBUG_DIR)/consumer: shm_Consum_QueueOps.cpp $(COMMON_SOURCES) $(HEADERS) | $(DEBUG_DIR)
	$(CXX) $(CPPFLAGS) $(DEBUG_FLAGS) shm_Consum_QueueOps.cpp $(COMMON_SOURCES) -o $@

clean:
	rm -rf $(BUILD_DIR)
