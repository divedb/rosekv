cmake_minimum_required(VERSION 3.20)
project(rosekv LANGUAGES CXX)

if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 20)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  message(STATUS "setting C++ standard to C++${CMAKE_CXX_STANDARD}")
endif()

find_package(GTest REQUIRED)

set(ROSEKV_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/include")

add_subdirectory(tests)
add_subdirectory(third_party/kiwi)
