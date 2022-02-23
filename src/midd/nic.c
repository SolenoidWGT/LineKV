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
// struct list_head nic_send_list;
// static uint64_t nic_sendQ_lock = 0UL;
// static void memory_barrier();
// volatile bool nic_thread_ready = false;

// void
// nic_sending_queue_lock()
// {
// 	while (1)
// 	{
// 		if (__sync_bool_compare_and_swap((volatile uint64_t *)&nic_sendQ_lock, 0UL, 1UL))
// 			break;
// 	}
// }

// void
// nic_sending_queue_unlock()
// {
// 	memory_barrier();
// 	*(volatile uint64_t *)&nic_sendQ_lock = 0UL;
// }


// // struct dhmp_update_request*
// // make_up_update_request(struct mehcached_item *item, const uint8_t *value_base, uint64_t version, uint32_t value_length)
// // {
// //     // struct dhmp_update_request* req = (struct dhmp_update_request*) \
// //     //                 malloc(sizeof(struct dhmp_update_request));
// //     struct dhmp_update_request req;
// //     int next_node = (int)server_instance->server_id + 1;
// //     Assert(next_node != server_instance->node_nums);


// //     req.item = item;
// //     req.write_info.length = value_length;
// //     req.write_info.local_addr = value_base;
// //     req.write_info.mr = 
// //     req.write_info.rdma_trans = find_connect_server_by_nodeID(next_node);
// //     req.write_info.remote_addr = item->value_addr[];
// // }

// // list_for_each_entry(rdma_trans, &server_instance->client_list, client_entry)
// // list_add_tail(&new_trans->client_entry, &server_instance->client_list);
// // INIT_LIST_HEAD(&server_instance->client_list);


// // 将发送请求加入到发送链表后就返回，不会阻塞主线程
// void
// makeup_update_request(struct mehcached_item * item, uint64_t item_offset, const uint8_t *value, uint32_t value_length)
// {
//     Assert(!IS_TAIL(server_instance->server_type));
//     Assert(!IS_MIRROR(server_instance->server_type));

//     struct list_head * _new, * head, * next;
//     size_t next_id;
//     struct dhmp_update_request * up_req;

//     if (IS_MAIN(server_instance->server_type))
//         next_id = REPLICA_NODE_HEAD_ID;
//     else
//         next_id = server_instance->server_id + 1;

//     up_req = (struct dhmp_update_request *) malloc(sizeof(struct dhmp_update_request));

//     // 填充 dhmp_write_request 结构体
//     up_req->write_info.rdma_trans = find_connect_server_by_nodeID(next_id);
//     up_req->write_info.mr = mehcached_get_mapping_self_mr(item->mapping_id);
//     dump_mr(up_req->write_info.mr);
//     up_req->write_info.local_addr = (void*)value;
//     up_req->write_info.length = value_length;
//     up_req->write_info.remote_addr = item->remote_value_addr;
//     up_req->item_offset = item_offset;
//     up_req->item = item;

//     while(nic_thread_ready == false);

//     nic_sending_queue_lock();
//     list_add(&up_req->sending_list,  &nic_send_list);
//     // memory_barrier();
//     // _new = &up_req->sending_list;
//     // memory_barrier();
//     // head = &nic_send_list;
//     // __list_add(_new, head, head->next);
//     nic_sending_queue_unlock();

//     INFO_LOG("Node [%d] add send list", server_instance->server_id);
// }

// // NIC 只负责发送数据部分
// void * main_node_nic_thread(void * args)
// {
//     struct list_head nic_local_send_list;

//     pthread_detach(pthread_self());
//     INIT_LIST_HEAD(&nic_send_list);
//     INIT_LIST_HEAD(&nic_local_send_list);

//     nic_thread_ready = true;
//     INFO_LOG("Node [%d] start nic thread!", server_instance->server_id);
//     for(;;)
//     {
//         struct list_head *iter_node, *temp_node;
//         struct dhmp_update_request * send_req=NULL, * temp_send_req=NULL;
//         int re;
    
//         // 拷贝发送链表头节点，并重新初始化发送链表为空
//         // 减少锁的争抢
//         nic_sending_queue_lock();

//         list_replace(&nic_send_list, &nic_local_send_list);
//         memory_barrier();
//         nic_send_list.next = &nic_send_list;
//         memory_barrier();
//         nic_send_list.prev = &nic_send_list;
//         nic_sending_queue_unlock();

//         /**
//          * list_for_each_entry_safe - iterate over list of given type safe against removal of list entry
//          * @pos:	the type * to use as a loop cursor.
//          * @n:		another type * to use as temporary storage
//          * @head:	the head for your list.
//          * @member:	the name of the list_struct within the struct.
//          */
//         list_for_each_entry_safe(send_req, temp_send_req, &nic_local_send_list, sending_list)
//         {
//             INFO_LOG("NIC send remote_addr %lu", send_req->write_info.remote_addr);
//             re = dhmp_rdma_write_packed(&send_req->write_info);
//             if (re == -1)
//             {
//                 ERROR_LOG("NIC dhmp_rdma_write_packed error!,exit");
//                 Assert(false);
//             }
//             //增加一个双边操作用于通知，模仿 hyperloop 的行为
//             // 这样就不需要轮询了
//             mica_replica_update_notify(send_req->item_offset);

//             list_del(&send_req->sending_list);
//         }

//         // 将本地头节点初始化为空
//         INIT_LIST_HEAD(&nic_local_send_list);
//     }
//     pthread_exit(0);
// }

// void set_main_node_thread_addr(void* (**p)(void*))
// {
//     *p = main_node_nic_thread;
// }

// void set_replica_node_thread_addr(void* (**p)(void*))
// {
//     *p = main_node_nic_thread;
// }