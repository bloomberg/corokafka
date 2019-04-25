include(GoogleTest)
set(TEST_TARGET ${PROJECT_NAME}_tests)
file(GLOB SOURCE_FILES *.cpp)
add_executable(${TEST_TARGET} ${SOURCE_FILES})
add_dependencies(${TEST_TARGET} ${PROJECT_NAME})
target_include_directories(${TEST_TARGET} BEFORE PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}
)
target_include_directories(${TEST_TARGET} SYSTEM BEFORE PUBLIC
    ${CPPKAFKA_INCLUDE_DIR}
    ${RDKAFKA_INCLUDE_DIR}
    ${Boost_INCLUDE_DIRS}
    ${QUANTUM_INCLUDE_DIR}
)
gtest_discover_tests(${TEST_TARGET}
                     WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(${TEST_TARGET}
    ${PROJECT_NAME}
    ${CPPKAFKA_LIBRARY}
    ${RDKAFKA_LIBRARY}
    ${Boost_LIBRARIES}
    GTest::GTest
    GTest::Main
    pthread rt ssl crypto dl z
)
set_target_properties(${TEST_TARGET}
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests"
    RUNTIME_OUTPUT_NAME "${TEST_TARGET}.${CMAKE_SYSTEM_NAME}${MODE}"
)
if (COROKAFKA_VERBOSE_MAKEFILE)
    message(STATUS "SOURCE_FILES = ${SOURCE_FILES}")
    get_property(inc_dirs DIRECTORY PROPERTY INCLUDE_DIRECTORIES)
    get_property(link_dirs DIRECTORY PROPERTY LINK_DIRECTORIES)
    message(STATUS "INCLUDE_DIRECTORIES = ${inc_dirs}")
    message(STATUS "LINK_DIRECTORIES = ${link_dirs}")
endif()
add_custom_target(tests)
add_dependencies(tests ${TEST_TARGET})
add_test(${PROJECT_NAME} ${TEST_TARGET})
