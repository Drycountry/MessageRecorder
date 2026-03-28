cmake_minimum_required(VERSION 3.20)

project(
    message_recorder
    VERSION 0.1.0
    LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(CTest)

add_library(
    message_recorder
    include/jojo/rec/inspect.hpp
    include/jojo/rec/recorder.hpp
    include/jojo/rec/replay.hpp
    include/jojo/rec/types.hpp
    src/inspect.cpp
    src/recorder.cpp
    src/replay.cpp
    src/internal/file_util.cpp
    src/internal/manifest.cpp
    src/internal/segment.cpp)

target_include_directories(
    message_recorder
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/3rdparty)

target_compile_features(message_recorder PUBLIC cxx_std_17)

if(MSVC)
  target_compile_options(message_recorder PRIVATE /W4 /permissive-)
else()
  target_compile_options(message_recorder PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_executable(
    message_recorder_cli
    cli/main.cpp)

target_link_libraries(message_recorder_cli PRIVATE message_recorder)

add_executable(
    message_recorder_benchmark
    benchmarks/main.cpp)

target_link_libraries(message_recorder_benchmark PRIVATE message_recorder)

if(BUILD_TESTING)
  add_executable(
      message_recorder_tests
      tests/test_main.cpp
      tests/test_helpers.cpp
      tests/test_helpers.hpp
      tests/test_cases.cpp)

  target_link_libraries(message_recorder_tests PRIVATE message_recorder)

  add_test(NAME message_recorder_tests COMMAND message_recorder_tests)
endif()