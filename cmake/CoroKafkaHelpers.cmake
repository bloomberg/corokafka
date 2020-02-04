# Create the public header for this project
function(make_header)
    set(MAIN_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/${PROJECT_TARGET_NAME}.h)
    file(GLOB INCLUDE_HEADERS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
            "*.h"
            "detail/*.h"
            "utils/*.h"
            "third_party/cppkafka/*.h")
    SET(PROJECT_INCLUDE_HEADERS  "${INCLUDE_HEADERS}" CACHE INTERNAL "${PROJECT_NAME} include headers")
    list(SORT INCLUDE_HEADERS)
    foreach(header ${INCLUDE_HEADERS})
        if (NOT ${header} STREQUAL "${PROJECT_TARGET_NAME}.h")
            SET(ALL_HEADERS "${ALL_HEADERS}#include <${PROJECT_TARGET_NAME}/${header}>\n")
        endif()
    endforeach()

    #create file from template
    configure_file(${PROJECT_SOURCE_DIR}/cmake/${PROJECT_TARGET_NAME}.h.in ${MAIN_HEADER} @ONLY)
endfunction()

function(set_root package PACKAGE)
    if (${PACKAGE}_ROOT_DIR)
        set(${package}_ROOT ${${PACKAGE}_ROOT_DIR}} PARENT_SCOPE)
    elseif (${PACKAGE}_ROOT)
        set(${package}_ROOT ${${PACKAGE}_ROOT} PARENT_SCOPE)
    endif()

    if (${package}_ROOT)
        if (NOT IS_ABSOLUTE ${${package}_ROOT})
            set(${package}_ROOT "${CMAKE_SOURCE_DIR}/${RdKafka_ROOT}" PARENT_SCOPE)
        endif()
        if (COROKAFKA_VERBOSE_MAKEFILE)
            message(STATUS "${package}_ROOT = ${${package}_ROOT}")
        endif()
    endif()

    if (${PACKAGE}_DIR)
        set(${package}_DIR ${${PACKAGE}_DIR} PARENT_SCOPE) # For older versions of find_package
        if (NOT IS_ABSOLUTE ${${package}_ROOT})
            set(${package}_DIR "${CMAKE_SOURCE_DIR}/${${package}_DIR}" PARENT_SCOPE)
        endif()
        if (COROKAFKA_VERBOSE_MAKEFILE)
            message(STATUS "${package}_DIR = ${${package}_DIR}")
        endif()
    endif()
endfunction()

