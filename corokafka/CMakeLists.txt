cmake_minimum_required(VERSION 3.10.3)
project(CoroKafka LANGUAGES CXX)
set(PROJECT_TARGET_NAME corokafka)
if (${CMAKE_VERSION} VERSION_GREATER_EQUAL "3.12.0")
    # Use <package>_ROOT variable to find configuration files
    cmake_policy(SET CMP0074 NEW)
endif()

# Ensure we have a proper build type
if (NOT CMAKE_BUILD_TYPE)
    message(WARNING "CMAKE_BUILD_TYPE not set. Using Debug.")
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type")
endif()
if (COROKAFKA_VERBOSE_MAKEFILE)
    message(STATUS "Build type set to ${CMAKE_BUILD_TYPE}")
endif()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cmake/")

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CoroKafkaHelpers)

set(CMAKE_FIND_PACKAGE_PREFER_CONFIG TRUE) #CMake > 3.15

# Set the version number.
set(COROKAFKA_VERSION_MAJOR 0)
set(COROKAFKA_VERSION_MINOR 2)
set(COROKAFKA_VERSION_PATCH 0)
set(COROKAFKA_VERSION "${COROKAFKA_VERSION_MAJOR}.${COROKAFKA_VERSION_MINOR}.${COROKAFKA_VERSION_PATCH}")
if (COROKAFKA_VERBOSE_MAKEFILE)
    message(STATUS "Current library version: ${COROKAFKA_VERSION}")
endif()

# CMake options
option(COROKAFKA_VERBOSE_MAKEFILE "Enable verbose cmake output." ON)
option(COROKAFKA_BUILD_SHARED "Build corokafka as a shared library." OFF)
option(COROKAFKA_ENABLE_PIC "Enable position independent code for shared library builds." OFF)
option(COROKAFKA_CPPKAFKA_STATIC_LIB "Link with cppkafka static library." ON)
option(COROKAFKA_RDKAFKA_STATIC_LIB "Link with rdkafka static library." ON)
option(COROKAFKA_BUILD_DOC "Build documentation." OFF)
option(COROKAFKA_ENABLE_DOT "Enable generation of DOT files." OFF)
option(COROKAFKA_ENABLE_TESTS "Generate tests target." OFF)
option(COROKAFKA_BOOST_STATIC_LIBS "Link with Boost static libraries." ON)
option(COROKAFKA_BOOST_USE_MULTITHREADED "Use Boost multithreaded libraries." ON)
option(COROKAFKA_BOOST_USE_VALGRIND "Use valgrind headers for Boost." OFF)
option(COROKAFKA_EXPORT_PKGCONFIG "Generate 'quantum.pc' file" ON)
option(COROKAFKA_EXPORT_CMAKE_CONFIG "Generate CMake config, target and version files." ON)

if (COROKAFKA_INSTALL_ROOT)
    set(CMAKE_INSTALL_PREFIX ${COROKAFKA_INSTALL_ROOT})
endif()

# RdKafka library root
set_root(RdKafka RDKAFKA)
set_root(CppKafka CPPKAFKA)
set_root(Quantum QUANTUM)
set_root(${PROJECT_NAME} COROKAFKA)

message(STATUS "RdKafka_ROOT = [${RdKafka_ROOT}]")

#Global options
set(LINKER_LANGUAGE CXX)
set(CMAKE_INCLUDE_DIRECTORIES_BEFORE ON)

#Set the compiler if the CXX environment variable is not set
if (NOT CMAKE_CXX_COMPILER)
    set(CMAKE_CXX_COMPILER g++)
endif()

#Set the compiler if the CXX_STANDARD environment variable is not set
if (NOT CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 14)
endif()

#Set compile flags if CXXFLAGS environment variable is not set
if (NOT CMAKE_CXX_FLAGS)
    set(CMAKE_CXX_FLAGS "-Wall -Wextra ${WERROR} -O0 -m64 -std=c++${CMAKE_CXX_STANDARD} -ftemplate-backtrace-limit=0")
endif()

if (COROKAFKA_VERBOSE_MAKEFILE)
    message(STATUS "CMAKE_CXX_FLAGS = ${CMAKE_CXX_FLAGS}")
endif()

# Set RPATH preferences
if (COROKAFKA_BUILD_SHARED)
    if (NOT DEFINED CMAKE_SKIP_BUILD_RPATH)
        SET(CMAKE_SKIP_BUILD_RPATH FALSE)
    endif()
    if (NOT DEFINED CMAKE_BUILD_WITH_INSTALL_RPATH)
        SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
    endif()
endif()

if (NOT COROKAFKA_PKGCONFIG_DIR)
    set(COROKAFKA_PKGCONFIG_DIR share/pkgconfig)
endif()

if (NOT COROKAFKA_CMAKE_CONFIG_DIR)
    set(COROKAFKA_CMAKE_CONFIG_DIR share/cmake/${PROJECT_NAME})
endif()

add_definitions(
    -D_REENTRANT
    -D_THREAD_SAFE
    -D_POSIX_PTHREAD_SEMANTICS
    -D__FUNCTION__=__FILE__
)

if(COROKAFKA_BUILD_SHARED)
    message(STATUS "Build will generate a shared library. "
            "Use COROKAFKA_BUILD_SHARED=OFF to perform a static build")
    set(COROKAFKA_LIBRARY_TYPE SHARED)
else()
    message(STATUS "Build will generate a static library.")
    set(COROKAFKA_LIBRARY_TYPE STATIC)
endif()

if (COROKAFKA_BUILD_DOC)
    message(STATUS "Generating Doxygen configuration files")
    # Add a target to generate API documentation using Doxygen
    find_package(Doxygen)
    if(DOXYGEN_FOUND)
        # Configure Doxygen parameters
        set(DOXYGEN_PROJECT_NAME "CoroKafka Library")
        set(DOXYGEN_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR})
        set(DOXYGEN_INPUT ${PROJECT_SOURCE_DIR}/${PROJECT_TARGET_NAME})
        set(DOXYGEN_HTML_OUTPUT ${PROJECT_SOURCE_DIR}/docs)
        set(DOXYGEN_CREATE_SUBDIRS YES)
        if (DOXYGEN_DOT_FOUND AND COROKAFKA_ENABLE_DOT)
            set(DOXYGEN_HAVE_DOT YES)
        else()
            set(DOXYGEN_HAVE_DOT NO)
        endif()
        # set input and output files
        set(DOXYGEN_IN ${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in)
        set(DOXYGEN_OUT ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)
        configure_file(${DOXYGEN_IN} ${DOXYGEN_OUT} @ONLY)
        add_custom_target(docs ALL
            COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_OUT}
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
            COMMENT "Generating API documentation with Doxygen"
            VERBATIM)
    endif()
else()
    message(STATUS "Doxygen configuration files have not been generated. Use COROKAFKA_BUILD_DOC=ON to generate them.")
endif()

# Look for Boost
find_package(Boost 1.61 REQUIRED COMPONENTS context)
if (Boost_FOUND)
    if (COROKAFKA_BOOST_USE_VALGRIND)
        add_definitions(-DBOOST_USE_VALGRIND)
    endif()
    set(Boost_USE_STATIC_LIBS ${COROKAFKA_BOOST_STATIC_LIBS})
    set(Boost_USE_MULTITHREADED ${COROKAFKA_BOOST_USE_MULTITHREADED})
    if (COROKAFKA_VERBOSE_MAKEFILE)
        message(STATUS "Boost include dir: ${Boost_INCLUDE_DIRS}")
        message(STATUS "Boost library dir: ${Boost_LIBRARY_DIRS}")
        message(STATUS "Boost use static libs: ${Boost_USE_STATIC_LIBS}")
        message(STATUS "Boost is multi-threaded: ${Boost_USE_MULTITHREADED}")
        message(STATUS "Boost libraries: ${Boost_LIBRARIES}")
    endif()
else()
    message(FATAL_ERROR "Boost not found, please define BOOST_ROOT")
endif()

# Find the quantum library installation
find_package(Quantum CONFIG REQUIRED)
if (NOT Quantum_FOUND)
    message(FATAL_ERROR "Could not find Quantum library. Please set QUANTUM_ROOT to the Quantum install folder.")
else()
    message(STATUS "Found Quantum library.")
endif()

# Find the cppkafka library installation
find_package(CppKafka CONFIG REQUIRED)
if (NOT CppKafka_FOUND)
    message(FATAL_ERROR "Could not find CppKafka library. Please set CPPKAFKA_ROOT to the CppKafka install folder.")
else()
    message(STATUS "Found CppKafka library.")
endif()

add_subdirectory(${PROJECT_TARGET_NAME})

if (COROKAFKA_ENABLE_TESTS)
    find_package(GTest REQUIRED)
    if (GTest_FOUND)
        message(STATUS "Adding target '${PROJECT_NAME}Tests' to build output")
        enable_testing()
        add_subdirectory(tests)
    else()
        message(STATUS "Package GTEST not found. Skipping testing.")
    endif()
else()
    message(STATUS "Skipping target 'tests'")
endif()

# Debug info
if (COROKAFKA_VERBOSE_MAKEFILE)
    message(STATUS "PROJECT_SOURCE_DIR = ${PROJECT_SOURCE_DIR}/")
    message(STATUS "CMAKE_INSTALL_PREFIX = ${CMAKE_INSTALL_PREFIX}")
    message(STATUS "REQUIRED BOOST_VERSION = 1.61")
endif()
