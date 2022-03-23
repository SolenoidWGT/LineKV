#include "dhmp.h"
#include "common.h"
#include "table.h"
#include "shm.h"
#include "dhmp_log.h"
#include "dhmp_transport.h"
#include "dhmp_server.h"
#include "dhmp_mica_shm_common.h"
#include "nic.h"
#include "util.h"

#include "mica_partition.h"

struct list_head tmp_send_list;   
struct list_head nic_send_list[PARTITION_MAX_NUMS];
static volatile uint64_t nic_sendQ_lock[PARTITION_MAX_NUMS];
static void memory_barrier();
volatile bool nic_thread_ready = false;

// 非对齐的 64bit int 是否是原子读写？
volatile int nic_list_length = 0;

struct ibv_mr * 
mehcached_get_mapping_self_mr(struct replica_mappings * mappings, size_t mapping_id)
{
	return &mappings->mrs[mapping_id];
}

void
nic_sending_queue_lock(int lock_id)
{
	while (1)
	{
		if (__sync_bool_compare_and_swap((volatile uint64_t *)(&nic_sendQ_lock[lock_id]), 0UL, 1UL))
			break;
	}
}

void
nic_sending_queue_unlock(int lock_id)
{
	memory_barrier();
	*(volatile uint64_t *)(&nic_sendQ_lock[lock_id]) = 0UL;
}

// 将发送请求加入到发送链表后就返回，不会阻塞主线程
void
makeup_update_request(struct mehcached_item * item, uint64_t item_offset, const uint8_t *value, uint32_t value_length, size_t tag)
{
    Assert(false);
    item;
    item_offset;
    value;
    value_length;
    tag;
}

// NIC 只负责发送数据部分
void * main_node_nic_thread(void * args)
{
    Assert(false);
    args;
    pthread_exit(0);
}

void set_main_node_thread_addr(void* (**p)(void*))
{
    *p = main_node_nic_thread;
}

void set_replica_node_thread_addr(void* (**p)(void*))
{
    *p = main_node_nic_thread;
}

void resp_filter(struct dhmp_mica_set_request  * req_info)
{
	void * key_addr   = (void*)req_info->data;
	void * value_addr = (void*)key_addr + req_info->key_length;
}