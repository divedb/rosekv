cmake_minimum_required(VERSION 3.20)
project(rosekv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(ROSEKV_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/include")

add_subdirectory()

add_subdirectory(tests)
