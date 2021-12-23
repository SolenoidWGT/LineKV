#include <stdio.h>
#include <Assert.h>
#include <sys/mman.h>

#include "mehcached.h"
#include "hash.h"


#include "dhmp.h"
#include "dhmp_log.h"
#include "dhmp_hash.h"
#include "dhmp_config.h"
#include "dhmp_context.h"
#include "dhmp_dev.h"
#include "dhmp_transport.h"
#include "dhmp_task.h"

#include "dhmp_client.h"
#include "dhmp_server.h"
#include "dhmp_init.h"
#include "mid_rdma_utils.h"

#define TEST_MMAP_SIZE 1024

void
test_basic()
{
    INFO_LOG("test_basic()\n");
    struct dhmp_server * mid_server;
    struct dhmp_device * dev;
    mid_server = dhmp_server_init();


    dev=dhmp_get_dev_from_server();
    void *p = mmap(NULL, TEST_MMAP_SIZE, PROT_READ | PROT_WRITE | PROT_EXEC, MAP_PRIVATE  | MAP_ANONYMOUS, -1, 0);
    struct ibv_mr * mr=ibv_reg_mr(dev->pd, p, TEST_MMAP_SIZE, 
                                    IBV_ACCESS_LOCAL_WRITE|
									IBV_ACCESS_REMOTE_READ|
									IBV_ACCESS_REMOTE_WRITE|
									IBV_ACCESS_REMOTE_ATOMIC);
    if(!mr)
	{
		ERROR_LOG("rdma register memory error. register mem length is [%u], error number is [%d], reason is \"%s\", addr is %p",  TEST_MMAP_SIZE, errno, strerror(errno), p);
		exit(0);
	}

    struct mehcached_table table_o;
    struct mehcached_table *table = &table_o;
    size_t numa_nodes[] = {(size_t)-1};
    mehcached_table_init(table, 1, 1, 256, false, false, false, numa_nodes[0], numa_nodes, MEHCACHED_MTH_THRESHOLD_FIFO);
    Assert(table);

    size_t i;
    for (i = 0; i < MEHCACHED_ITEMS_PER_BUCKET; i++)
    {
        size_t key = i;
        size_t value = i;
        uint64_t key_hash = hash((const uint8_t *)&key, sizeof(key));
        //INFO_LOG("add key = %zu, value = %zu, key_hash = %lx\n", key, value, key_hash);

        if (!mehcached_set(0, table, key_hash, (const uint8_t *)&key, sizeof(key), (const uint8_t *)&value, sizeof(value), 0, false))
            Assert(false);
    }
    for (i = 0; i < MEHCACHED_ITEMS_PER_BUCKET; i++)
    {
        size_t key = i;
        size_t value = 100 + i;
        uint64_t key_hash = hash((const uint8_t *)&key, sizeof(key));
        //INFO_LOG("set key = %zu, value = %zu, key_hash = %lx\n", key, value, key_hash);

        if (!mehcached_set(0, table, key_hash, (const uint8_t *)&key, sizeof(key), (const uint8_t *)&value, sizeof(value), 0, true))
            Assert(false);
    }

    size_t value = 0;
    for (i = 0; i < MEHCACHED_ITEMS_PER_BUCKET; i++)
    {
        size_t key = i;
        uint64_t key_hash = hash((const uint8_t *)&key, sizeof(key));

        size_t value_length = sizeof(value);
        if (!mehcached_get(0, table, key_hash, (const uint8_t *)&key, sizeof(key), (uint8_t *)&value, &value_length, NULL, false))
        {
            INFO_LOG("get key = %zu, value = <not found>\n", key);
            continue;
        }
        Assert(value_length == sizeof(value));
        INFO_LOG("get key = %zu, value = %zu\n", key, value);
    }

    mehcached_print_stats(table);

    mehcached_table_free(table);
}

int
main(int argc MEHCACHED_UNUSED, const char *argv[] MEHCACHED_UNUSED)
{
	const size_t page_size = 1048576 * 2;
	const size_t num_numa_nodes = 2;
    const size_t num_pages_to_try = 16384;
    const size_t num_pages_to_reserve = 16384 - 2048;   // give 2048 pages to dpdk

	// mehcached_shm_init(page_size, num_numa_nodes, num_pages_to_try, num_pages_to_reserve);

    // test_basic();

    return EXIT_SUCCESS;
}

