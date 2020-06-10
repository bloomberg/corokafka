# Local function to auto-generate main corokafka.h header file
cmake_minimum_required(VERSION 3.10.3)

# Run file generation function
make_header()

file(GLOB SOURCE_FILES
    *.cpp
    detail/*.cpp
    impl/*.cpp
    utils/*.cpp
    mock/*.cpp
    interface/*.cpp
)

set(PKG_DIR             "${CMAKE_BINARY_DIR}/package")
set(PKG_CONFIG_FILE     "${PKG_DIR}/${PROJECT_TARGET_NAME}.pc")
set(CONFIG_FILE         "${PKG_DIR}/${PROJECT_NAME}Config.cmake")
set(VERSION_FILE        "${PKG_DIR}/${PROJECT_NAME}ConfigVersion.cmake")
set(NAMESPACE           "${PROJECT_NAME}::")
set(TARGET_EXPORT_NAME  ${PROJECT_NAME}Targets)

message(STATUS "SOURCE_FILES=[${SOURCE_FILES}]")
add_library(${PROJECT_TARGET_NAME} ${COROKAFKA_LIBRARY_TYPE} ${SOURCE_FILES})
target_include_directories(${PROJECT_TARGET_NAME}
    BEFORE PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
)
set_target_properties(${PROJECT_TARGET_NAME}
    PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_INSTALL_LIBDIR}"
    ARCHIVE_OUTPUT_NAME "${PROJECT_TARGET_NAME}"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_INSTALL_LIBDIR}"
    LIBRARY_OUTPUT_NAME "${PROJECT_TARGET_NAME}"
    VERSION ${COROKAFKA_VERSION}
    SOVERSION ${COROKAFKA_VERSION}
    INSTALL_RPATH "${CMAKE_INSTALL_LIBDIR}"
    INSTALL_RPATH_USE_LINK_PATH TRUE
    POSITION_INDEPENDENT_CODE ON
)

target_link_libraries(${PROJECT_TARGET_NAME} PUBLIC
    CppKafka::cppkafka
    Quantum::quantum
)

# Install cppkafka target and specify all properties needed for the exported file
install(
    TARGETS ${PROJECT_TARGET_NAME}
    EXPORT ${TARGET_EXPORT_NAME}
    COMPONENT binaries
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)

# Install headers including the auto-generated corokafka.h
install(DIRECTORY
        ${CMAKE_CURRENT_SOURCE_DIR}
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
        FILES_MATCHING PATTERN "*.h")

if (COROKAFKA_EXPORT_PKGCONFIG)
    # Generate and install pkgconfig file
    configure_file(${PROJECT_SOURCE_DIR}/cmake/${PROJECT_TARGET_NAME}.pc.in ${PKG_CONFIG_FILE} @ONLY)

    install(
        FILES ${PKG_CONFIG_FILE}
        DESTINATION "${COROKAFKA_PKGCONFIG_DIR}"
        COMPONENT pkgconfig
    )
endif()

if (COROKAFKA_EXPORT_CMAKE_CONFIG)
    # Install the exported file
    install(
        EXPORT "${TARGET_EXPORT_NAME}"
        NAMESPACE "${NAMESPACE}"
        DESTINATION "${COROKAFKA_CMAKE_CONFIG_DIR}"
        COMPONENT config
    )

    # Generate CMAKE configuration file and exported targets
    configure_package_config_file(
        "${PROJECT_SOURCE_DIR}/cmake/${PROJECT_NAME}Config.cmake.in"
        "${CONFIG_FILE}"
        INSTALL_DESTINATION "${COROKAFKA_CMAKE_CONFIG_DIR}"
        PATH_VARS CMAKE_INSTALL_PREFIX CMAKE_INSTALL_INCLUDEDIR CMAKE_INSTALL_LIBDIR
    )

    # Generate version file
    write_basic_package_version_file(
        "${VERSION_FILE}"
        VERSION ${COROKAFKA_VERSION}
        COMPATIBILITY AnyNewerVersion
    )

    # Install config and version files
    install(
        FILES "${CONFIG_FILE}" "${VERSION_FILE}"
        DESTINATION "${COROKAFKA_CMAKE_CONFIG_DIR}"
        COMPONENT config
    )
endif()


