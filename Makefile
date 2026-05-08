.PHONY: build build-asan build-tsan build-fuzz build-coverage test test-tsan test-coverage \
        lint chaos bench fuzz docker clean format tidy

BUILD_DIR ?= build
BUILD_TYPE ?= Release

build:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE)
	cmake --build $(BUILD_DIR) -j

build-asan:
	cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DIR_ENABLE_ASAN=ON
	cmake --build build-asan -j

build-tsan:
	cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DIR_ENABLE_TSAN=ON \
		-DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++
	cmake --build build-tsan -j

build-coverage:
	cmake -S . -B build-cov -DCMAKE_BUILD_TYPE=Debug -DIR_ENABLE_COVERAGE=ON \
		-DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++
	cmake --build build-cov -j

build-fuzz:
	CC=clang CXX=clang++ \
	CFLAGS="-fsanitize=fuzzer-no-link,address,undefined -fno-omit-frame-pointer -g -O1" \
	CXXFLAGS="-fsanitize=fuzzer-no-link,address,undefined -fno-omit-frame-pointer -g -O1" \
	LDFLAGS="-fsanitize=fuzzer-no-link,address,undefined" \
	cmake -S . -B build-fuzz -DCMAKE_BUILD_TYPE=Debug -DIR_BUILD_FUZZ=ON \
		-DIR_BUILD_TESTS=OFF -DIR_BUILD_BENCH=OFF
	cmake --build build-fuzz -j --target fuzz_wire

test: build
	ctest --test-dir $(BUILD_DIR) --output-on-failure

test-asan: build-asan
	ASAN_OPTIONS=detect_leaks=0:abort_on_error=1:halt_on_error=1 \
	UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
	ctest --test-dir build-asan --output-on-failure

test-tsan: build-tsan
	TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1 \
	ctest --test-dir build-tsan --output-on-failure --timeout 180

test-coverage: build-coverage
	ctest --test-dir build-cov --output-on-failure
	@which lcov >/dev/null 2>&1 || (echo 'lcov not installed; apt-get install -y lcov' && exit 1)
	lcov --capture --directory build-cov --output-file build-cov/coverage.info \
		--rc geninfo_unexecuted_blocks=1 --ignore-errors mismatch,gcov,unused \
		--exclude '*/_deps/*' --exclude '*/tests/*' --exclude '/usr/*'
	lcov --list build-cov/coverage.info | tee build-cov/coverage-summary.txt

fuzz: build-fuzz
	./build-fuzz/fuzz_wire -max_total_time=15 tests/fuzz/corpus

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
