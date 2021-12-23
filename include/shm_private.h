#ifndef SHM_PRIVATE_H
#define SHM_PRIVATE_H

#include <fcntl.h>
#include <sys/mman.h>
#include <linux/limits.h>
#include <sys/stat.h>

#include "dhmp_mica_shm_common.h"

struct mehcached_shm_entry
{
	size_t refcount;	// reference by mapping
	size_t to_remove;	// remove entry when refcount == 0
	size_t length;
	size_t num_pages;
	size_t *pages;
};

struct mehcached_shm_page
{
	char path[PATH_MAX];
	void *addr;
	void *paddr;
	size_t numa_node;
	size_t in_use;
};

void copy_mapping_info(void * src);
void copy_mapping_mrs_info(struct ibv_mr * mrs);
inline size_t get_mapping_nums();
#endif