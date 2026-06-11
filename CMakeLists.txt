cmake_minimum_required(VERSION 3.18)

project(
    sprint
    VERSION 0.0.1
    DESCRIPTION "sprint ipc library"
    LANGUAGES CXX
)

if(NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 17)
endif()

set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Project options

option(SPRINT_ENABLE_CUDA "Enable CUDA support if it is available" ON)
option(SPRINT_BUILD_DEMO "Build sprint demos" OFF)


# cuda (if option set)
include(cmake/cuda.cmake)

add_subdirectory(sprint/ipc)

if (SPRINT_BUILD_DEMO)
    add_subdirectory(sprint/ipc/demo)
endif()