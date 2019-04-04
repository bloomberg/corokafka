# From https://github.com/mfontanini/cppkafka/blob/master/cmake/FindRdKafka.cmake
find_path(QUANTUM_ROOT_DIR
    NAMES include/quantum/quantum.h
)
find_path(QUANTUM_INCLUDE_DIR
    NAMES quantum/quantum.h
    HINTS ${QUANTUM_ROOT_DIR}/include
)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(QUANTUM DEFAULT_MSG
    QUANTUM_INCLUDE_DIR
)
# Don't expose these variables
mark_as_advanced(
    QUANTUM_INCLUDE_DIR
)
