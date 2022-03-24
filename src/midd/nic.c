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
    Assert(!IS_TAIL(server_instance->server_type));
    Assert(!IS_MIRROR(server_instance->server_type));

    struct list_head * _new, * head, * next;
    size_t next_id;
    struct dhmp_update_request * up_req;
    int nic_partition_id;

    if (IS_MAIN(server_instance->server_type))
        next_id = REPLICA_NODE_HEAD_ID;
    else
        next_id = server_instance->server_id + 1;

    up_req = (struct dhmp_update_request *) malloc(sizeof(struct dhmp_update_request));

    // 填充 dhmp_write_request 结构体
    up_req->write_info.rdma_trans = find_connect_server_by_nodeID(next_id);
    up_req->write_info.mr = mehcached_get_mapping_self_mr(next_node_mappings, item->mapping_id);
    dump_mr(up_req->write_info.mr);
    up_req->write_info.local_addr = (void*)value;
    up_req->write_info.length = value_length;
    up_req->write_info.remote_addr = item->remote_value_addr;
    up_req->item_offset = item_offset;
    up_req->item = item;
    up_req->partition_id = (uint16_t)(item->key_hash >> 48) & (uint16_t)(PARTITION_NUMS  - 1);
    up_req->tag = tag;

    while(nic_thread_ready == false);

#ifdef NIC_MULITI_THREAD
    nic_partition_id = up_req->partition_id;
#else
    nic_partition_id = 0;
#endif
    
    nic_sending_queue_lock(nic_partition_id);
    // memory_barrier();
    list_add(&up_req->sending_list,  &nic_send_list[nic_partition_id]);
    nic_list_length++;
    nic_sending_queue_unlock(nic_partition_id);

    INFO_LOG("Node [%d] add send list", server_instance->server_id);
}

// NIC 只负责发送数据部分
void * main_node_nic_thread(void * args)
{
    int partition_id = (int) (uintptr_t)args;
    int i;

    if (partition_id == 0)
    {
        for (i=0 ;i<PARTITION_NUMS; i++)
            nic_sendQ_lock[i] = 0UL;
    }

    INFO_LOG("NIC thread [%d] launch!", partition_id);
    pthread_detach(pthread_self());
    INIT_LIST_HEAD(&nic_send_list[partition_id]);
    INIT_LIST_HEAD(&tmp_send_list);

    nic_thread_ready = true;
    INFO_LOG("Node [%d] start nic thread!", server_instance->server_id);
    for(;;)
    {
        struct list_head *iter_node, *temp_node;
        int re;
        struct dhmp_update_request * send_req=NULL, * temp_send_req=NULL;

        // 链表的长度不要太短，有足够多的key后再尝试获取锁
        // 这是什么傻逼设计？？
        // if (nic_list_length < 10)
        //     continue;

        // 拷贝发送链表头节点，并重新初始化发送链表为空
        // 减少锁的争抢
        nic_sending_queue_lock(partition_id);
        //memory_barrier();
        list_replace(&nic_send_list[partition_id], &tmp_send_list); // 将 nic_send_list 的内容转移到 tmp_send_list 上
        INIT_LIST_HEAD(&nic_send_list[partition_id]);   // 清空 nic_send_list 链表
        nic_sending_queue_unlock(partition_id);

        /**
         * list_for_each_entry_safe - iterate over list of given type safe against removal of list entry
         * @pos:	the type * to use as a loop cursor.
         * @n:		another type * to use as temporary storage
         * @head:	the head for your list.
         * @member:	the name of the list_struct within the struct.
         */
        list_for_each_entry_safe(send_req, temp_send_req, &tmp_send_list, sending_list)
        {
            INFO_LOG("NIC send remote_addr %lu", send_req->write_info.remote_addr);
            void * key = item_get_key_addr(send_req->item);

            re = dhmp_rdma_write_packed(&send_req->write_info);
            if (re == -1)
            {
                ERROR_LOG("NIC dhmp_rdma_write_packed error!,exit");
                Assert(false);
            }
            //增加一个双边操作用于通知，模仿 hyperloop 的行为
            // 这样就不需要轮询了
            mica_replica_update_notify(send_req->item_offset, send_req->partition_id, send_req->tag);

            list_del(&send_req->sending_list);

            // 防止内存泄漏
            free(send_req);
            nic_list_length--;
        }

        // 将本地头节点初始化为空
        INIT_LIST_HEAD(&tmp_send_list);
    }
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

// dhmp_mica_set_request_handler
void resp_filter(struct dhmp_mica_set_request  * req_info)
{
	void * key_addr   = (void*)req_info->data;
	void * value_addr = (void*)key_addr + req_info->key_length;
}