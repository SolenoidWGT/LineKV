// Copyright 2014 Carnegie Mellon University
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "shm.h"
#include "util.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <linux/limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include "dhmp_log.h"

#ifdef USE_DPDK
#include <rte_lcore.h>
#include <rte_debug.h>
#endif

MEHCACHED_BEGIN

struct mehcached_shm_page
{
	char path[PATH_MAX];
	void *addr;
	void *paddr;
	size_t numa_node;
	size_t in_use;
};

struct mehcached_shm_entry
{
	size_t refcount;	// reference by mapping
	size_t to_remove;	// remove entry when refcount == 0
	size_t length;
	size_t num_pages;
	size_t *pages;
};

struct mehcached_shm_mapping
{
	size_t entry_id;
	void *addr;
	size_t length;
	size_t page_offset;
	size_t num_pages;
};

#define MEHCACHED_SHM_MAX_PAGES (65536)
#define MEHCACHED_SHM_MAX_ENTRIES (8192)
#define MEHCACHED_SHM_MAX_MAPPINGS (16384)

static size_t mehcached_shm_page_size;
static uint64_t mehcached_shm_state_lock;
static struct mehcached_shm_page mehcached_shm_pages[MEHCACHED_SHM_MAX_PAGES];
static struct mehcached_shm_entry mehcached_shm_entries[MEHCACHED_SHM_MAX_ENTRIES];
static struct mehcached_shm_mapping mehcached_shm_mappings[MEHCACHED_SHM_MAX_MAPPINGS];
static size_t mehcached_shm_used_memory;

static const char *mehcached_shm_path_prefix = "/mnt/huge/mehcached_shm_";

size_t
mehcached_shm_adjust_size(size_t size)
{
    return (uint64_t)MEHCACHED_ROUNDUP2M(size);
}

static
void
mehcached_clean_files()
{
	char cmd[PATH_MAX];
	snprintf(cmd, PATH_MAX, "rm %s* > /dev/null 2>&1", mehcached_shm_path_prefix);
	int ret = system(cmd);
	(void)ret;
}

static
void
mehcached_shm_path(size_t page_id, char out_path[PATH_MAX])
{
	snprintf(out_path, PATH_MAX, "%s%zu", mehcached_shm_path_prefix, page_id);
}


void
mehcached_shm_lock()
{
	while (1)
	{
		if (__sync_bool_compare_and_swap((volatile uint64_t *)&mehcached_shm_state_lock, 0UL, 1UL))
			break;
	}
}


void
mehcached_shm_unlock()
{
	memory_barrier();
	*(volatile uint64_t *)&mehcached_shm_state_lock = 0UL;
}

void
mehcached_shm_dump_page_info()
{
	mehcached_shm_lock();
	size_t page_id;
	for (page_id = 0; page_id < MEHCACHED_SHM_MAX_PAGES; page_id++)
	{
		if (mehcached_shm_pages[page_id].addr == NULL)
			continue;

		INFO_LOG("page %zu: addr=%p numa_node=%zu in_use=%zu\n", page_id, mehcached_shm_pages[page_id].addr, mehcached_shm_pages[page_id].numa_node, mehcached_shm_pages[page_id].in_use);
	}
	mehcached_shm_unlock();
}

static
int
mehcached_shm_compare_paddr(const void *a, const void *b)
{
	const struct mehcached_shm_page *pa = (const struct mehcached_shm_page *)a;
	const struct mehcached_shm_page *pb = (const struct mehcached_shm_page *)b;
	if (pa->paddr < pb->paddr)
		return -1;
	else
		return 1;
}

static
int
mehcached_shm_compare_vaddr(const void *a, const void *b)
{
	const struct mehcached_shm_page *pa = (const struct mehcached_shm_page *)a;
	const struct mehcached_shm_page *pb = (const struct mehcached_shm_page *)b;
	if (pa->addr < pb->addr)
		return -1;
	else
		return 1;
}

void
mehcached_shm_init(size_t page_size, size_t num_numa_nodes, size_t num_pages_to_try, size_t num_pages_to_reserve)
{
    INFO_LOG("NOTICE: mehcached_shm_init not work! : %u %u %u %u", page_size, num_numa_nodes, num_pages_to_try, num_pages_to_reserve);
	assert(mehcached_next_power_of_two(page_size) == page_size);
	assert(num_numa_nodes >= 1);
	assert(num_pages_to_try <= MEHCACHED_SHM_MAX_PAGES);
	assert(num_pages_to_reserve <= num_pages_to_try);

	// necessary init
	mehcached_shm_state_lock = 0;
	mehcached_shm_page_size = page_size;
	memset(mehcached_shm_pages, 0, sizeof(mehcached_shm_pages));
	// memset(mehcached_shm_entries, 0, sizeof(mehcached_shm_entries));
	memset(mehcached_shm_mappings, 0, sizeof(mehcached_shm_mappings));
	mehcached_shm_used_memory = 0;
}

void *
mehcached_shm_find_free_address(size_t size)
{
	size_t alignment = mehcached_shm_page_size;

	if (alignment == 0)
		alignment = 1;

	if (mehcached_next_power_of_two(alignment) != alignment)
	{
		INFO_LOG("invalid alignment\n");
		return NULL;
	}

	int fd = open("/dev/zero", O_RDONLY);
	if (fd == -1)
	{
		perror("");
		assert(false);
		return NULL;
	}

	void *p = mmap(NULL, size + alignment, PROT_READ, MAP_PRIVATE, fd, 0);

	close(fd);

	if (p == (void *)-1)
	{
		perror("");
		return NULL;
	}

	munmap(p, size);

	p = (void *)(((size_t)p + (alignment - 1)) & ~(alignment - 1));
	return p;
}

size_t
mehcached_shm_alloc(size_t length, size_t numa_node)
{
	INFO_LOG("");
    INFO_LOG("NOTICE: mehcached_shm_alloc not work! : %u %u", length, numa_node);
    return 0;
}

static
void
mehcached_shm_check_remove(size_t entry_id)
{
    INFO_LOG("NOTICE: mehcached_shm_check_remove not work! : %u", entry_id);

}

bool
mehcached_shm_schedule_remove(size_t entry_id)
{
    INFO_LOG("NOTICE: mehcached_shm_schedule_remove not work! : %u", entry_id);
    return true;
}

size_t
mehcached_shm_map(size_t entry_id, void *ptr, void ** bucket_ptr, 
					size_t offset, size_t length, bool table_init MEHCACHED_UNUSED)
{
	// ptr 的起始地址必须和 page 大小对齐
	if (((size_t)ptr & ~(mehcached_shm_page_size - 1)) != (size_t)ptr)
	{
		ERROR_LOG("invalid ptr alignment");
		exit(0);
		return (size_t)-1;
	}

	// offset 也必须要和 page 大小对齐
	if ((offset & ~(mehcached_shm_page_size - 1)) != offset)
	{
		ERROR_LOG("invalid offset alignment");
		return (size_t)-1;
	}

	// 如果在初始化阶段，则不加锁，防止锁重入错误
	mehcached_shm_lock();

	// find empty mapping
	size_t mapping_id;
	for (mapping_id = 0; mapping_id < MEHCACHED_SHM_MAX_MAPPINGS; mapping_id++)
	{
		if (mehcached_shm_mappings[mapping_id].addr == NULL)
			break;
	}

	if (mapping_id == MEHCACHED_SHM_MAX_MAPPINGS)
	{
		ERROR_LOG("too many mappings");
		mehcached_shm_unlock();
		return (size_t)-1;
	}

	size_t page_offset = offset / mehcached_shm_page_size; 
	size_t num_pages = (length + (mehcached_shm_page_size - 1)) / mehcached_shm_page_size;

	// map
	void *p = ptr;
	size_t page_index = page_offset;
	// size_t page_index_end = page_offset + num_pages;
	int error = 0;

    size_t total_alloc_pages = mehcached_shm_page_size * num_pages;
    // 映射到匿名的地址空间上去
    // void *ret_p = mmap(p, total_alloc_pages, PROT_READ | PROT_WRITE,  MAP_FIXED | MAP_ANONYMOUS, -1, 0);
	void * ret_p = malloc(total_alloc_pages);
	p = ret_p;
	*bucket_ptr = ret_p;

    if (ret_p == MAP_FAILED)
    {
        ERROR_LOG("mmap failed at %p", p);
        error = 1;
    }

	if (error)
	{
		// clean partialy mapped memory
		p = ptr;
		size_t page_index_clean = page_offset;
		while (page_index_clean < page_index)
		{
			munmap(p, mehcached_shm_page_size);
			page_index_clean++;
			p = (void *)((size_t)p + mehcached_shm_page_size);
		}
		mehcached_shm_unlock();
		return (size_t)-1;
	}

	// register mapping
	mehcached_shm_used_memory += num_pages * mehcached_shm_page_size;

	mehcached_shm_mappings[mapping_id].entry_id = entry_id;
	mehcached_shm_mappings[mapping_id].addr = ret_p;
	mehcached_shm_mappings[mapping_id].length = length;
	mehcached_shm_mappings[mapping_id].page_offset = page_offset;
	mehcached_shm_mappings[mapping_id].num_pages = num_pages;

	// 如果在初始化阶段，则不加锁，防止锁重入错误
	mehcached_shm_unlock();

#ifndef NDEBUG
	INFO_LOG("created new mapping %zu (shm entry %zu, page_offset=%zu, num_pages=%zu) at %p", mapping_id, entry_id, page_offset, num_pages, ptr);
#endif

	return mapping_id;
}

bool
mehcached_shm_unmap(void *ptr)
{
	mehcached_shm_lock();

	// find mapping
	size_t mapping_id;
	for (mapping_id = 0; mapping_id < MEHCACHED_SHM_MAX_MAPPINGS; mapping_id++)
	{
		if (mehcached_shm_mappings[mapping_id].addr == ptr)
			break;
	}

	if (mapping_id == MEHCACHED_SHM_MAX_MAPPINGS)
	{
		INFO_LOG("invalid unmap\n");
		mehcached_shm_unlock();
		return false;
	}

	// unmap pages
	size_t page_index;
	for (page_index = 0; page_index < mehcached_shm_mappings[mapping_id].num_pages; page_index++)
	{
		munmap(ptr, mehcached_shm_page_size);
		ptr = (void *)((size_t)ptr + mehcached_shm_page_size);
	}

	// remove reference to entry
	--mehcached_shm_entries[mehcached_shm_mappings[mapping_id].entry_id].refcount;
	mehcached_shm_check_remove(mehcached_shm_mappings[mapping_id].entry_id);

	// remove mapping
	memset(&mehcached_shm_mappings[mapping_id], 0, sizeof(mehcached_shm_mappings[mapping_id]));

	mehcached_shm_unlock();

#ifndef NDEBUG
	INFO_LOG("removed mapping %zu at %p\n", mapping_id, ptr);
#endif

	return true;
}

size_t
mehcached_shm_get_page_size()
{
	return mehcached_shm_page_size;
}

size_t
mehcached_shm_get_memuse()
{
	return mehcached_shm_used_memory;
}

MEHCACHED_END

