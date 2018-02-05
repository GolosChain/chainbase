# Defines ChainBase library target.
project(ChainBase)
cmake_minimum_required(VERSION 2.8.12)


if(POLICY CMP0042)
    cmake_policy(SET CMP0042 NEW)
endif()

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

find_package(Boost 1.58 REQUIRED COMPONENTS ${BOOST_COMPONENTS})

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


list(APPEND HEADERS
        #include/chainbase/cached_level_map.hpp
        include/chainbase/datastream_back.hpp
        include/chainbase/exception.hpp
        include/chainbase/fast_level_map.hpp
        include/chainbase/flat_index.hpp
        include/chainbase/fwd.hpp
        include/chainbase/generic_index.hpp
        include/chainbase/index.hpp
        include/chainbase/level_map.hpp
        #include/chainbase/level_pod_map.hpp
        include/chainbase/object.hpp
        include/chainbase/object_database.hpp
        include/chainbase/object_id.hpp
        include/chainbase/simple_index.hpp
        include/chainbase/undo_database.hpp
        include/chainbase/upgrade_leveldb.hpp
)

list(APPEND SOURCES
        src/index.cpp
        src/object_database.cpp
        src/undo_database.cpp
        src/upgrade_leveldb.cpp
)

set( LEVEL_DB_DIR "${CMAKE_SOURCE_DIR}/leveldb" )

file( GLOB LEVEL_DB_SOURCES "${LEVEL_DB_DIR}/db/*.cc"
        "${LEVEL_DB_DIR}/helpers/memenv/memenv.cc"
        "${LEVEL_DB_DIR}/table/*.cc"
        "${LEVEL_DB_DIR}/util/*.cc" )
foreach( filename ${LEVEL_DB_SOURCES} )
    if( ${filename} MATCHES ".*_test.cc" OR ${filename} MATCHES ".*_bench.cc" OR ${filename} MATCHES ".*_main.cc" )
        list( REMOVE_ITEM LEVEL_DB_SOURCES ${filename} )
    endif()
endforeach()
set(LEVELDB_BUILD_DEFINES)
set(LEVELDB_BUILD_LIBRARIES)
set(LEVELDB_BUILD_PRIVATE_INCLUDES "${LEVEL_DB_DIR}")

SET(LEVELDB_PORT_FILE "${LEVEL_DB_DIR}/port/port_posix.cc" )

list(APPEND LEVELDB_BUILD_DEFINES LEVELDB_PLATFORM_POSIX LEVELDB_ATOMIC_PRESENT)
if( APPLE )
    list(APPEND LEVELDB_BUILD_DEFINES OS_MACOSX)
else() # Linux
    list(APPEND LEVELDB_BUILD_DEFINES OS_LINUX)
    list(APPEND LEVELDB_BUILD_LIBRARIES pthread)
endif()

list(APPEND LEVEL_DB_SOURCES "${LEVELDB_PORT_FILE}")
add_library( leveldb ${LEVEL_DB_SOURCES} )
target_link_libraries( leveldb ${LEVELDB_BUILD_LIBRARIES} )
target_include_directories( leveldb PRIVATE ${LEVELDB_BUILD_PRIVATE_INCLUDES} PUBLIC "${LEVEL_DB_DIR}/include" )
set_target_properties(leveldb PROPERTIES COMPILE_DEFINITIONS "${LEVELDB_BUILD_DEFINES}")

include(ExternalProject)

# Find external libraries
# zlib
message (STATUS "Checking for zlib")
find_package(ZLIB REQUIRED)
include_directories(SYSTEM ${ZLIB_INCLUDE_DIRS})

# bzip2
message (STATUS "Checking for bzip2")
find_package (BZip2 REQUIRED)
include_directories(SYSTEM ${BZIP_INCLUDE_DIRS})

# snappy
#ExternalProject_Add(
#        project_snappy
#        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/snappy
#        BUILD_IN_SOURCE 1
#        PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/snappy
#        CONFIGURE_COMMAND ""
#        BUILD_COMMAND $(MAKE)
#        INSTALL_COMMAND ""
#)

#ExternalProject_Get_Property(project_snappy install_dir)
#add_library(snappy STATIC IMPORTED)
#include_directories(SYSTEM ${install_dir})

#set(snappy_location ${install_dir}/.libs/libsnappy.a)
#message(STATUS "snappy target location is ${snappy_location}")
#set_property(TARGET snappy PROPERTY IMPORTED_LOCATION ${snappy_location})
#add_dependencies(snappy project_snappy)

## rocksdb

#ExternalProject_Add(
#        project_rocksdb
#        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb
#        BUILD_IN_SOURCE 1
#        PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb
#        CONFIGURE_COMMAND ""
#        BUILD_COMMAND PORTABLE=1;CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/snappy $(MAKE) -C ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb DISABLE_JEMALLOC=1 static_lib
#        INSTALL_COMMAND ""
#)

ExternalProject_Add(
        project_rocksdb
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb
        BUILD_IN_SOURCE 1
        PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb
        CONFIGURE_COMMAND ""
        BUILD_COMMAND PORTABLE=1;CFLAGS= $(MAKE) -C ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb DISABLE_JEMALLOC=1 static_lib
        INSTALL_COMMAND ""
)

ExternalProject_Get_Property(project_rocksdb install_dir)
add_library(rocksdb STATIC IMPORTED)
include_directories(SYSTEM ${install_dir} ${install_dir}/include)
set_property(TARGET rocksdb PROPERTY IMPORTED_LOCATION ${install_dir}/librocksdb.a)
#add_dependencies(project_rocksdb project_snappy snappy)
#add_dependencies(rocksdb project_rocksdb project_snappy snappy)



if(BUILD_SHARED_LIBRARIES)
    add_library(
            chainbase SHARED
            ${HEADERS}
            ${SOURCES}
    )

else()
    add_library(
            chainbase STATIC
            ${HEADERS}
            ${SOURCES}
    )

endif()



target_link_libraries(
        chainbase
        ${Boost_LIBRARIES}
        rocksdb
        #snappy
        ${ZLIB_LIBRARIES}
        leveldb
        ${BZIP2_LIBRARIES}
)

target_include_directories(
        chainbase PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        ${Boost_INCLUDE_DIR}
        ${ROCKSDB_INCLUDE_DIR}

)

add_subdirectory(test)

install(TARGETS
        chainbase

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        )
install(FILES ${HEADERS} DESTINATION "include/")