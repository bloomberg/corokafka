# Local function to auto-generate main corokafka.h header file
cmake_minimum_required(VERSION 3.10.3)
function(make_header)
    set(MAIN_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/${PROJECT_NAME}.h)
    file(GLOB INCLUDE_HEADERS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
            "*.h"
            "detail/*.h")
    list(SORT INCLUDE_HEADERS)
    foreach(header ${INCLUDE_HEADERS})
        if (NOT ${header} MATCHES "${PROJECT_NAME}.h")
            SET(ALL_HEADERS "${ALL_HEADERS}#include <${PROJECT_NAME}/${header}>\n")
        endif()
    endforeach()

    #create file from template
    configure_file(${PROJECT_SOURCE_DIR}/${PROJECT_NAME}.h.in ${MAIN_HEADER} @ONLY)
endfunction()

# Run file generation function
make_header()

file(GLOB SOURCE_FILES
    *.cpp
    detail/*.cpp
    impl/*.cpp
)

add_library(${PROJECT_NAME} ${COROKAFKA_LIBRARY_TYPE} ${SOURCE_FILES})
target_include_directories(${PROJECT_NAME} BEFORE PRIVATE
    ${PROJECT_SOURCE_DIR}
)
target_include_directories(${PROJECT_NAME} SYSTEM PUBLIC
                ${PROJECT_SOURCE_DIR}/../../quantum/quantum
    ${CPPKAFKA_INCLUDE_DIR}
    ${RDKAFKA_INCLUDE_DIR}
    ${Boost_INCLUDE_DIRS}
   # ${QUANTUM_INCLUDE_DIR}
)

# Properly set the output directory
if (${BITNESS} EQUAL -1)
    set(LIBDIR "lib")
else()
    set(LIBDIR "lib64")
endif()

set_target_properties(${PROJECT_NAME}
    PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${LIBDIR}"
    ARCHIVE_OUTPUT_NAME "${PROJECT_NAME}"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${LIBDIR}"
    LIBRARY_OUTPUT_NAME "${PROJECT_NAME}"
    VERSION ${COROKAFKA_VERSION}
    SOVERSION ${COROKAFKA_VERSION}
)

if (COROKAFKA_BUILD_SHARED)
    if (COROKAFKA_ENABLE_PIC)
        set_target_properties(${PROJECT_NAME}
            PROPERTIES
            POSITION_INDEPENDENT_CODE ON)
    endif()
endif()

target_link_libraries(${PROJECT_NAME}
    ${CPPKAFKA_LIBRARY}
    ${Boost_LIBRARIES}
)

# Install headers including the auto-generated corokafka.h
file(GLOB INCLUDE_FILES "*.h")
file(GLOB DETAIL_INCLUDE_FILES "detail/*.h")
file(GLOB IMPL_INCLUDE_FILES "impl/*.h")

install(
    FILES ${INCLUDE_FILES}
    DESTINATION include/${PROJECT_NAME}
    COMPONENT headers
)
install(
    FILES ${DETAIL_INCLUDE_FILES}
    DESTINATION include/${PROJECT_NAME}/detail
    COMPONENT headers
)
install(
    FILES ${IMPL_INCLUDE_FILES}
    DESTINATION include/${PROJECT_NAME}/impl
    COMPONENT headers
)

# Library binary install
install(
    TARGETS ${PROJECT_NAME}
    LIBRARY DESTINATION ${LIBDIR}
    ARCHIVE DESTINATION ${LIBDIR}
    COMPONENT library
)
