# From https://github.com/mfontanini/cppkafka/blob/master/cmake/FindRdKafka.cmake
# Override default CMAKE_FIND_LIBRARY_SUFFIXES
if (COROKAFKA_RDKAFKA_STATIC_LIB)
    set(RDKAFKA_PREFIX ${CMAKE_STATIC_LIBRARY_PREFIX})
    set(RDKAFKA_SUFFIX ${CMAKE_STATIC_LIBRARY_SUFFIX})
else()
    set(RDKAFKA_PREFIX ${CMAKE_SHARED_LIBRARY_PREFIX})
    set(RDKAFKA_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
endif()

find_path(RDKAFKA_ROOT_DIR
    NAMES include/librdkafka/rdkafka.h
)

find_path(RDKAFKA_INCLUDE_DIR
    NAMES librdkafka/rdkafka.h
    HINTS ${RDKAFKA_ROOT_DIR}/include
)

# Check lib paths
if (COROKAFKA_CMAKE_VERBOSE)
    get_property(FIND_LIBRARY_32 GLOBAL PROPERTY FIND_LIBRARY_USE_LIB32_PATHS)
    get_property(FIND_LIBRARY_64 GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS)
    message(STATUS "RDKAFKA search 32-bit library paths: ${FIND_LIBRARY_32}")
    message(STATUS "RDKAFKA search 64-bit library paths: ${FIND_LIBRARY_64}")
endif()

find_library(RDKAFKA_LIBRARY
    NAMES ${RDKAFKA_PREFIX}rdkafka${RDKAFKA_SUFFIX} rdkafka
    HINTS ${RDKAFKA_ROOT_DIR}/lib ${RDKAFKA_ROOT_DIR}/lib64
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RDKAFKA DEFAULT_MSG
    RDKAFKA_LIBRARY
    RDKAFKA_INCLUDE_DIR
)
# Don't expose these variables
mark_as_advanced(
    RDKAFKA_ROOT_DIR
    RDKAFKA_INCLUDE_DIR
    RDKAFKA_LIBRARY
)
