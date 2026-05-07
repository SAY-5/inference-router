.PHONY: build build-asan test lint chaos bench docker clean format tidy

BUILD_DIR ?= build
BUILD_TYPE ?= Release

build:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE)
	cmake --build $(BUILD_DIR) -j

build-asan:
	cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DIR_ENABLE_ASAN=ON
	cmake --build build-asan -j

test: build
	ctest --test-dir $(BUILD_DIR) --output-on-failure

test-asan: build-asan
	ASAN_OPTIONS=detect_leaks=0:abort_on_error=1:halt_on_error=1 \
	UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
	ctest --test-dir build-asan --output-on-failure

chaos: build
	$(BUILD_DIR)/chaos --clients 50 --requests 100 --sigterm-after-ms 5000

chaos-smoke: build
	$(BUILD_DIR)/chaos --clients 10 --requests 50 --sigterm-after-ms 1500

bench: build
	$(BUILD_DIR)/load_bench --concurrency 16 --duration-s 5

bench-smoke: build
	$(BUILD_DIR)/load_bench --concurrency 8 --requests 1000

format:
	find src tools tests bench -name '*.cpp' -o -name '*.h' | xargs clang-format -i

format-check:
	find src tools tests bench -name '*.cpp' -o -name '*.h' | xargs clang-format --dry-run -Werror

tidy: build
	clang-tidy -p $(BUILD_DIR) src/connection.cpp src/thread_pool.cpp src/backend_pool.cpp src/metrics.cpp

docker:
	docker build -t inference-router:local .

clean:
	rm -rf build build-asan
