# Defines ChainBase library target.
project(ChainBase)
cmake_minimum_required(VERSION 2.8.12)

#list( APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMakeModules" )

set(CMAKE_EXPORT_COMPILE_COMMANDS "ON")

set(BOOST_COMPONENTS)
list(APPEND BOOST_COMPONENTS thread
        date_time
        system
        filesystem
        chrono
        unit_test_framework
        locale)

option(Boost_USE_STATIC_LIBS "Build with Boost static libraries usage" TRUE)

option(BUILD_SHARED_LIBRARIES "Build shared libraries" TRUE)

if(WIN32)
    set(BOOST_ROOT $ENV{BOOST_ROOT})
    set(Boost_USE_MULTITHREADED ON)
    set(BOOST_ALL_DYN_LINK OFF) # force dynamic linking for all libraries
endif(WIN32)

find_package(Boost 1.57 REQUIRED COMPONENTS ${BOOST_COMPONENTS})

if(APPLE)
    # Apple Specific Options Here
    message(STATUS "Configuring ChainBase on OS X")
    set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -std=c++11 -stdlib=libc++ -Wall -Wno-conversion")
else(APPLE)
    # Linux Specific Options Here
    message(STATUS "Configuring ChainBase on Linux")
    set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -std=c++11 -Wall")
    set(rt_library rt)
    set(pthread_library pthread)
    if(FULL_STATIC_BUILD)
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++ -static-libgcc")
    endif(FULL_STATIC_BUILD)
endif(APPLE)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp")
endif()

if("${CMAKE_GENERATOR}" STREQUAL "Ninja")
    if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcolor-diagnostics")
    endif()
endif()

# based on http://www.delorie.com/gnu/docs/gdb/gdb_70.html
# uncomment this line to tell GDB about macros (slows compile times)
# set( CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -gdwarf-2 -g3" )

set(ENABLE_COVERAGE_TESTING FALSE CACHE BOOL "Build ChainBase for code coverage analysis")

if(ENABLE_COVERAGE_TESTING)
    set(CMAKE_CXX_FLAGS "--coverage ${CMAKE_CXX_FLAGS}")
endif()


file(GLOB HEADERS "include/*.hpp")
if(BUILD_SHARED_LIBRARIES)
    add_library(chainbase SHARED
            src/chainbase.cpp
            ${HEADERS})
else()
    add_library(chainbase STATIC
            src/chainbase.cpp
            ${HEADERS})
endif()
target_link_libraries(chainbase ${Boost_LIBRARIES})
target_include_directories(chainbase PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" ${Boost_INCLUDE_DIR})

add_subdirectory(test)

install(TARGETS
        chainbase

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        )
install(FILES ${HEADERS} DESTINATION "include/")