cmake_minimum_required(VERSION 3.17)
project(SSDEngine)

set(CMAKE_CXX_STANDARD 17)
add_subdirectory(src)
include_directories(${CMAKE_SOURCE_DIR}/include)