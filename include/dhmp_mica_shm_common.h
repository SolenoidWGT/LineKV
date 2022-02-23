#ifndef DHMP_MICA_SHM_COMMON_H
#define DHMP_MICA_SHM_COMMON_H
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <math.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <sys/time.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <numa.h>

#include "./linux/list.h"
#include "json-c/json.h"
#include "basic_types.h"
#include "common.h"
#include "table.h"

#define MEHCACHED_SHM_MAX_PAGES (65536)
#define MEHCACHED_SHM_MAX_ENTRIES (8192)
#define MEHCACHED_SHM_MAX_MAPPINGS (16384)
struct mehcached_shm_mapping
{
	size_t entry_id;
	void *addr;
	size_t length;
	size_t page_offset;
	size_t num_pages;
#ifdef USE_RDMA
	// 存放本节点的MR，同时存放下游节点的MR，头节点MR[0]为NULL，尾节点MR[1]为NULL
	struct ibv_mr * mr;  
#endif
};

struct replica_mappings
{
	bool first_inited;
	int node_id;
	size_t used_mapping_nums;
	struct mehcached_shm_mapping mehcached_shm_pages[MEHCACHED_SHM_MAX_MAPPINGS];
	struct ibv_mr 			 	 mrs[MEHCACHED_SHM_MAX_MAPPINGS];
};


void copy_mapping_info(void * src);
void copy_mapping_mrs_info(struct ibv_mr * mrs);
inline size_t get_mapping_nums();


struct ibv_mr * mehcached_get_mapping_self_mr(size_t mapping_id);
void makeup_update_request(const uint8_t *key, size_t key_length, const uint8_t *value, size_t value_length);
#endif