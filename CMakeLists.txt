# Copyright 2014 Carnegie Mellon University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 2.6)
project(MIDD_MICA) 

if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_CURRENT_BINARY_DIR)
	message(FATAL_ERROR "Use out-of-source build only!")
endif(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_CURRENT_BINARY_DIR)

# add support for rdma
SET(CMAKE_BUILD_TYPE "Debug")  
include_directories(/usr/local/include/libxml2/)
include_directories(/home/rdma_match/xinzh/include)
link_directories(/home/rdma_match/xinzh/lib)

include_directories(include)

add_subdirectory(src)

add_custom_target(
    symbolic_links
    ALL
    ln -sf src/test src/load src/microbench src/microbench_store src/netbench_client src/netbench_server src/netbench_client_latency src/netbench_client_soft_fdir src/netbench_server_latency src/netbench_server_soft_fdir src/netbench_server_store src/netbench_analysis .
)

