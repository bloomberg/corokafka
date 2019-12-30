include(GoogleTest)
set(TEST_TARGET ${PROJECT_NAME}Tests)
file(GLOB SOURCE_FILES *.cpp)
add_executable(${TEST_TARGET} ${SOURCE_FILES})
set_target_properties(${TEST_TARGET}
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests"
    RUNTIME_OUTPUT_NAME "${TEST_TARGET}.${CMAKE_SYSTEM_NAME}"
)

# Find program options component
find_package(Boost 1.61 REQUIRED COMPONENTS program_options)

if (COROKAFKA_BUILD_TESTS_FROM_INSTALL_TREE)
    # Find CoroKafka
    find_package(CoroKafka REQUIRED)
    target_link_libraries(${TEST_TARGET}
        CoroKafka::corokafka
        Boost::program_options
        GTest::GTest)
else()
    add_dependencies(${TEST_TARGET} ${PROJECT_TARGET_NAME})
    target_include_directories(${TEST_TARGET} BEFORE PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}
    )
    target_include_directories(${TEST_TARGET} SYSTEM BEFORE PUBLIC
        ${QUANTUM_INCLUDE_DIR}
        ${CPPKAFKA_INCLUDE_DIR}
        ${RDKAFKA_INCLUDE_DIR}
        ${Boost_INCLUDE_DIRS}
    )
    target_link_libraries(${TEST_TARGET}
        ${PROJECT_TARGET_NAME}
        ${CPPKAFKA_LIBRARY}
        ${RDKAFKA_LIBRARY}
        Boost::context
        Boost::program_options
        GTest::GTest
        pthread
        rt
        ssl
        crypto
        dl
        z
        )
    if (COROKAFKA_VERBOSE_MAKEFILE)
        message(STATUS "SOURCE_FILES = ${SOURCE_FILES}")
        get_property(inc_dirs DIRECTORY PROPERTY INCLUDE_DIRECTORIES)
        get_property(link_dirs DIRECTORY PROPERTY LINK_DIRECTORIES)
        message(STATUS "INCLUDE_DIRECTORIES = ${inc_dirs}")
        message(STATUS "LINK_DIRECTORIES = ${link_dirs}")
    endif()
endif()

gtest_discover_tests(${TEST_TARGET}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
add_test(NAME ${TEST_TARGET}
         COMMAND ${TEST_TARGET})
