file(GLOB UNIT_TESTS "*.cpp")

if (NOT Boost_USE_STATIC_LIBS)
    add_definitions(-DBOOST_TEST_DYN_LINK)
endif()

add_executable(chainbase_test ${UNIT_TESTS})
target_link_libraries(chainbase_test chainbase ${PLATFORM_SPECIFIC_LIBS})