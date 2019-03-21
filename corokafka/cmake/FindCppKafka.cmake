# # From https://github.com/mfontanini/cppkafka/blob/master/cmake/FindRdKafka.cmake
# Override default CMAKE_FIND_LIBRARY_SUFFIXES
if (COROKAFKA_CPPKAFKA_STATIC_LIB)
    set(CPPKAFKA_PREFIX ${CMAKE_STATIC_LIBRARY_PREFIX})
    set(CPPKAFKA_SUFFIX ${CMAKE_STATIC_LIBRARY_SUFFIX})
else()
    set(CPPKAFKA_PREFIX ${CMAKE_SHARED_LIBRARY_PREFIX})
    set(CPPKAFKA_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
endif()

find_path(CPPKAFKA_ROOT_DIR
    NAMES include/cppkafka/cppkafka.h
)

find_path(CPPKAFKA_INCLUDE_DIR
    NAMES cppkafka/cppkafka.h
    HINTS ${CPPKAFKA_ROOT_DIR}/include
)

# Check lib paths
if (COROKAFKA_VERBOSE_MAKEFILE)
    get_property(FIND_LIBRARY_32 GLOBAL PROPERTY FIND_LIBRARY_USE_LIB32_PATHS)
    get_property(FIND_LIBRARY_64 GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS)
    MESSAGE(STATUS "CPPKAFKA search 32-bit library paths: ${FIND_LIBRARY_32}")
    MESSAGE(STATUS "CPPKAFKA search 64-bit library paths: ${FIND_LIBRARY_64}")
endif()

find_library(CPPKAFKA_LIBRARY
    NAMES ${CPPKAFKA_PREFIX}cppkafka${CPPKAFKA_SUFFIX} cppkafka
    HINTS ${CPPKAFKA_ROOT_DIR}/lib ${CPPKAFKA_ROOT_DIR}/lib64
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(CPPKAFKA DEFAULT_MSG
    CPPKAFKA_LIBRARY
    CPPKAFKA_INCLUDE_DIR
)
# Don't expose these variables
mark_as_advanced(
    CPPKAFKA_ROOT_DIR
    CPPKAFKA_INCLUDE_DIR
    CPPKAFKA_LIBRARY
)
