cmake_minimum_required(VERSION 3.10.3)
project(corokafka LANGUAGES CXX)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cmake/")

# Set the version number.
set(COROKAFKA_VERSION_MAJOR 0)
set(COROKAFKA_VERSION_MINOR 1)
set(COROKAFKA_VERSION "${COROKAFKA_VERSION_MAJOR}.${COROKAFKA_VERSION_MINOR}")

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
if (COROKAFKA_INSTALL_ROOT)
    set(CMAKE_INSTALL_PREFIX ${COROKAFKA_INSTALL_ROOT})
endif()
if (COROKAFKA_VERBOSE_MAKEFILE)
    message(STATUS "Current library version: ${COROKAFKA_VERSION}")
endif()

#Global options
set(LINKER_LANGUAGE CXX)
set(CMAKE_INCLUDE_DIRECTORIES_BEFORE ON)
set(CMAKE_BUILD_TYPE Debug)

#Set the compiler if the CXX environment variable is not set
if (NOT DEFINED ENV{CXX})
    set(CMAKE_CXX_COMPILER g++)
endif()
#Set the compiler if the CXX_STANDARD environment variable is not set
if (NOT DEFINED ENV{CXXSTANDARD})
    set(CMAKE_CXX_STANDARD 14)
else()
    set(CMAKE_CXX_STANDARD $ENV{CXXSTANDARD})
endif()
#Set compile flags if CXXFLAGS environment variable is not set
if (NOT DEFINED ENV{CXXFLAGS})
    set(CMAKE_CXX_FLAGS "-Wall -Wextra -Werror -Wabi -O0 -m64 -std=c++${CMAKE_CXX_STANDARD} -ftemplate-backtrace-limit=0")
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
    if (NOT DEFINED CMAKE_INSTALL_RPATH)
        SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib ${CMAKE_INSTALL_PREFIX}/lib64")
    endif()
    if (NOT DEFINED)
        SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
    endif()
endif()

if (NOT COROKAFKA_PKGCONFIG_DIR)
    set(COROKAFKA_PKGCONFIG_DIR share/pkgconfig)
endif()

# Determine if this is a 32 or 64 bit build
math(EXPR BITS "8*${CMAKE_SIZEOF_VOID_P}")

# Properly set the output directory
if (${BITS} EQUAL 64)
    set(MODE 64)
endif()
set(LIBDIR "lib${MODE}")

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
        set(DOXYGEN_INPUT ${PROJECT_SOURCE_DIR}/${PROJECT_NAME})
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
find_package(Quantum MODULE REQUIRED)
if (NOT QUANTUM_FOUND)
    message(STATUS "Quantum_FOUND = ${Quantum_FOUND}")
    message(FATAL_ERROR "Could not find Quantum library. Please set QUANTUM_ROOT_DIR properly.")
endif()

# Find the rdkafka library installation
find_package(RdKafka MODULE REQUIRED)
if (NOT RDKAFKA_FOUND)
    message(FATAL_ERROR "Could not find RdKafka library. Please set RDKAFKA_ROOT_DIR properly.")
endif()

# Find the cppkafka library installation
find_package(CppKafka MODULE REQUIRED)
if (NOT CPPKAFKA_FOUND)
    message(FATAL_ERROR "Could not find CppKafka library. Please set CPPKAFKA_ROOT_DIR properly.")
endif()

link_directories(
    ${CPPKAFKA_ROOT_DIR}
    ${RDKAFKA_ROOT_DIR}
    ${QUANTUM_ROOT_DIR}
    ${Boost_LIBRARY_DIRS}
)

add_subdirectory(${PROJECT_NAME})

if (COROKAFKA_ENABLE_TESTS)
    find_package(GTest REQUIRED)
    if (GTEST_FOUND)
        message(STATUS "Adding target 'corokafka_tests' to build output")
        enable_testing()
        add_subdirectory(tests)
    else()
        message(STATUS "Package GTEST not found. Skipping testing.")
    endif()
else()
    set(BUILD_TESTING OFF)
    message(STATUS "Skipping target 'tests'")
endif()

# Debug info
if (COROKAFKA_VERBOSE_MAKEFILE)
    message(STATUS "PROJECT_SOURCE_DIR = ${PROJECT_SOURCE_DIR}/")
    message(STATUS "CMAKE_INSTALL_PREFIX = ${CMAKE_INSTALL_PREFIX}")
    message(STATUS "BOOST_ROOT = ${BOOST_ROOT}")
    message(STATUS "REQUIRED BOOST_VERSION = 1.61")
    message(STATUS "GTEST_ROOT = ${GTEST_ROOT}")
    message(STATUS "RDKAFKA_ROOT_DIR = ${RDKAFKA_ROOT_DIR}")
    message(STATUS "CPPKAFKA_ROOT_DIR = ${CPPKAFKA_ROOT_DIR}")
    message(STATUS "QUANTUM_ROOT_DIR = ${QUANTUM_ROOT_DIR}")
endif()
