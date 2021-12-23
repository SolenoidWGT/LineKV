cmake_minimum_required(VERSION 2.6)
# basic configuration
if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_CURRENT_BINARY_DIR)
	message(FATAL_ERROR "Use out-of-source build only!")
endif(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_CURRENT_BINARY_DIR)

# compiler options
add_definitions(-pthread)
add_definitions(-g -Wall -Wextra -Wsign-conversion -Winline -Wno-unused-function)
add_definitions(-Wconversion)
add_definitions(-O9)
add_definitions(-msse4.2 -march=corei7)

# preprocessor
SET(NDEBUG "$ENV{NDEBUG}" CACHE STRING "define NDEBUG macro")

message(STATUS "NDEBUG (disable all additional checkings; no*, yes) = ${NDEBUG}")
if("${NDEBUG}" STREQUAL "yes")
	add_definitions(-DNDEBUG)
endif()

SET(NLOCAL "$ENV{NLOCAL}" CACHE STRING "no local programs")
SET(NCLIENT "$ENV{NCLIENT}" CACHE STRING "no clients")
SET(NSERVER "$ENV{NSERVER}" CACHE STRING "no servers")

message(STATUS "NLOCAL (do not compile local programs; no*, yes) = ${NLOCAL}")
message(STATUS "NCLIENT (do not compile clients; no*, yes) = ${NCLIENT}")
message(STATUS "NSERVER (do not compile server; no*, yes) = ${NSERVER}")

# common libraries
set(LIBRARIES ${LIBRARIES} rt crypto perf_count m pthread)
set(CMAKE_C_FLAGS ${CMAKE_C_FLAGS} "-msse2") 
set (EXECUTABLE_OUTPUT_PATH ${MIDD_MICA_SOURCE_DIR}/bin)
SET (LIBRARY_OUTPUT_PATH ${MIDD_MICA_SOURCE_DIR}/lib)

# 指定生成目标
if(NOT "${NLOCAL}" STREQUAL "yes")
	# test
    add_executable(ibtest ibtest.c)
    # 添加链接库
	target_link_libraries(ibtest ${LIBRARIES})
    # target_link_libraries(ibtest micalib)
    target_link_libraries(ibtest middlib)
    target_link_libraries(ibtest xml2)
    target_link_libraries(ibtest pthread)
    target_link_libraries(ibtest rdmacm)
    target_link_libraries(ibtest ibverbs)
    target_link_libraries(ibtest numa)
    target_link_libraries(ibtest rt)
    target_link_libraries(ibtest m)
    target_link_libraries(ibtest json-c)
    target_link_libraries(ibtest memkind)
endif()


