cmake_minimum_required(VERSION 2.8)

find_package(Boost 1.48.0)
find_package(PostgreSQL)

if (WIN32)
    add_definitions(_CRT_SECURE_NO_WARNINGS BOOST_ALL_NO_LIB)
endif (WIN32)

include_directories(${Boost_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_library(edba SHARED frontend.cpp backend.cpp utils.cpp)
