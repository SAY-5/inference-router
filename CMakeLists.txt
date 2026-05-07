cmake_minimum_required(VERSION 3.20)
project(inference_router CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

option(IR_ENABLE_ASAN "Build with AddressSanitizer + UBSan" OFF)
option(IR_BUILD_TESTS "Build unit/integration/chaos tests" ON)
option(IR_BUILD_BENCH "Build the benchmark binary" ON)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(WARNING
        "inference-router targets Linux (epoll). Building on ${CMAKE_SYSTEM_NAME} "
        "will fail at compile time. CI runs Ubuntu.")
endif()

set(IR_WARN_FLAGS -Wall -Wextra -Wpedantic -Werror -Wshadow -Wconversion -Wsign-conversion)

if(IR_ENABLE_ASAN)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=address,undefined)
endif()

add_library(ir_core STATIC
    src/connection.cpp
    src/thread_pool.cpp
    src/backend_pool.cpp
    src/acceptor.cpp
    src/handler.cpp
    src/shutdown.cpp
    src/metrics.cpp
    src/log.cpp
)
target_compile_options(ir_core PRIVATE ${IR_WARN_FLAGS})
target_include_directories(ir_core PUBLIC src)
target_link_libraries(ir_core PUBLIC pthread)

add_executable(inference-router src/main.cpp)
target_compile_options(inference-router PRIVATE ${IR_WARN_FLAGS})
target_link_libraries(inference-router PRIVATE ir_core)

add_executable(backend-echo tools/backend_echo/main.cpp)
target_compile_options(backend-echo PRIVATE ${IR_WARN_FLAGS})
target_link_libraries(backend-echo PRIVATE ir_core)

if(IR_BUILD_TESTS)
    include(FetchContent)
    FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz
        URL_HASH SHA256=8ad598c73ad796e0d8280b082cebd82a630d73e73cd3c70057938a6501bba5d7
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    enable_testing()

    # Tests opt out of -Wconversion/-Wsign-conversion to avoid noise from gtest macros.
    set(IR_TEST_WARN_FLAGS -Wall -Wextra -Wpedantic -Werror)

    add_executable(unit_tests
        tests/unit/test_connection.cpp
        tests/unit/test_thread_pool.cpp
        tests/unit/test_metrics.cpp
        tests/unit/test_backend_pool.cpp
    )
    target_compile_options(unit_tests PRIVATE ${IR_TEST_WARN_FLAGS})
    target_link_libraries(unit_tests PRIVATE ir_core GTest::gtest_main)
    add_test(NAME unit_tests COMMAND unit_tests)

    add_executable(integration_tests
        tests/integration/test_router_e2e.cpp
    )
    target_compile_options(integration_tests PRIVATE ${IR_TEST_WARN_FLAGS})
    target_link_libraries(integration_tests PRIVATE ir_core GTest::gtest_main)
    add_test(NAME integration_tests COMMAND integration_tests)
    set_tests_properties(integration_tests PROPERTIES TIMEOUT 60)

    add_executable(chaos tests/chaos/chaos.cpp)
    target_compile_options(chaos PRIVATE ${IR_WARN_FLAGS})
    target_link_libraries(chaos PRIVATE ir_core)
endif()

if(IR_BUILD_BENCH)
    add_executable(load_bench bench/load.cpp)
    target_compile_options(load_bench PRIVATE ${IR_WARN_FLAGS})
    target_link_libraries(load_bench PRIVATE ir_core)
endif()
