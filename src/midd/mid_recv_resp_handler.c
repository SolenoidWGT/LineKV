// "shm_private.h" 头文件要包含在 "dhmp_task.h" 等头文件之后
#define _GNU_SOURCE 1
#include "mica_partition.h"

#include "shm_private.h"
#include "table.h"
#include "dhmp_client.h"
#include "dhmp_log.h"
#include "dhmp_dev.h"
#include "dhmp_server.h"
#include "mid_rdma_utils.h"
#include "dhmp_transport.h"
#include "dhmp_task.h"
#include "table.h"
#include "util.h"
#include "alloc_dynamic.h"
#include "nic.h"

#include "dhmp_top_api.h"

//#define MAIN_LOG_DEBUG_LATENCE
#define MAIN_LOG_DEBUG_THROUGHOUT 
struct timespec start_through, end_through;

volatile bool replica_is_ready = false;

struct timespec start_set_g, end_set_g;
static uint64_t set_counts = 0, get_counts=0;
long long int total_set_time = 0, total_get_time =0;
long long int total_set_latency_time = 0, total_set_through_time=0;

static struct dhmp_msg * make_basic_msg(struct dhmp_msg * res_msg, struct post_datagram *resp);

struct mica_work_context * mica_work_context_mgr[2];

int __partition_nums;


// 由于 send 操作可能会被阻塞住，所以必须将 recv 操作让另一个线程处理，否则会出现死锁。
// 我们对每一个 partition 启动两个线程
void* mica_work_thread(void *data);

int get_req_partition_id(struct post_datagram *req);
int get_resp_partition_id(struct post_datagram *req);
void distribute_partition_resp(int partition_id, struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, enum dhmp_msg_type type, __time_t time_start1, __syscall_slong_t time_start2);

static void __dhmp_send_respone_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);
static void __dhmp_send_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);


static void  dhmp_set_response_handler(struct dhmp_msg* msg);
static struct post_datagram *  dhmp_mica_get_MR_request_handler(struct dhmp_transport* rdma_trans, struct post_datagram *req);

static
void
partition_lock(volatile uint64_t * lock)
{
	while (1)
	{
		if (__sync_bool_compare_and_swap(lock, 0UL, 1UL))
			break;
	}
}

static
void
partition_unlock(volatile uint64_t *lock)
{
	memory_barrier();
	*lock= 0UL;
}

static struct post_datagram * 
dhmp_mica_get_cli_MR_request_handler(struct dhmp_transport* rdma_trans,
									struct post_datagram *req)
{
	struct post_datagram *resp;
	struct dhmp_mica_get_cli_MR_request  * req_info;
	struct dhmp_mica_get_cli_MR_response * resp_req;
	struct dhmp_device * dev = dhmp_get_dev_from_server();
	size_t resp_len = sizeof(struct dhmp_mica_get_cli_MR_response);

	// 该节点的 mapping 信息和 mr 信息
	resp = (struct post_datagram *) malloc(DATAGRAM_ALL_LEN(resp_len));
	INFO_LOG("resp length is %u", DATAGRAM_ALL_LEN(resp_len));
	memset(resp, 0, DATAGRAM_ALL_LEN(resp_len));

	req_info  = (struct dhmp_mica_get_cli_MR_request *) DATA_ADDR(req, 0);
	resp_req  = (struct dhmp_mica_get_cli_MR_response *) DATA_ADDR(resp, 0);

	// 填充自身新产生的信息
	resp_req->resp_all_mapping.node_id = server_instance->server_id;
	resp_req->resp_all_mapping.used_mapping_nums = get_mapping_nums();
	resp_req->resp_all_mapping.first_inited = false;

	if (false == get_table_init_state())
	{
		mehcached_shm_lock();
		if (false == get_table_init_state() )
		{
			// 进行节点的 k-v 初始化
			struct mehcached_table *table = &table_o;
			size_t numa_nodes[] = {(size_t)-1};

			// 初始化 hash table 并注册内存
			mehcached_table_init(table, TABLE_BUCKET_NUMS, 1, TABLE_POOL_SIZE, true, true, true, \
								numa_nodes[0], numa_nodes, MEHCACHED_MTH_THRESHOLD_FIFO);
			Assert(table);

			resp_req->resp_all_mapping.first_inited = true;
			INFO_LOG("replica node[%d] first init finished!, caller is node[%d]", \
						server_instance->server_id, req->node_id);
			
			set_table_init_state(true);
		}
		mehcached_shm_unlock();
	}

	copy_mapping_info( (void*) resp_req->resp_all_mapping.mehcached_shm_pages);
	copy_mapping_mrs_info(&resp_req->resp_all_mapping.mrs[0]);

	// dump_mr(&resp_req->resp_all_mapping.mrs[1]);

	resp->req_ptr  = req->req_ptr;		    		// 原样返回对方用于消息完成时的报文的判别
	resp->resp_ptr = resp;							// 自身用于消息完成时报文的判别
	resp->node_id  = server_instance->server_id;	// 向对端发送自己的 node_id 用于身份辨识
	resp->info_type = MICA_GET_CLIMR_RESPONSE;
	resp->info_length = resp_len;
	resp->done_flag = false;						// request_handler 不关心 done_flag（不需要阻塞）

	return resp;
}

// 镜像节点调用该函数初始化自己的内存区域
static struct post_datagram * 
dhmp_mica_get_Mirr_MR_request_handler(struct dhmp_transport* rdma_trans,
									struct post_datagram *req)
{
	struct post_datagram *resp;
	struct dhmp_mica_get_cli_MR_request  * req_info;
	struct dhmp_mica_get_cli_MR_response * resp_req;
	struct dhmp_device * dev = dhmp_get_dev_from_server();
	size_t resp_len = sizeof(struct dhmp_mica_get_cli_MR_response);

	// Mirror arg
	struct dhmp_mr local_mr;
	int reval;

	resp = (struct post_datagram *) malloc(DATAGRAM_ALL_LEN(resp_len));
	INFO_LOG("resp length is %u", DATAGRAM_ALL_LEN(resp_len));
	memset(resp, 0, DATAGRAM_ALL_LEN(resp_len));

	req_info  = (struct dhmp_mica_get_cli_MR_request *) DATA_ADDR(req, 0);
	resp_req  = (struct dhmp_mica_get_cli_MR_response *) DATA_ADDR(resp, 0);

	// 填充自身新产生的信息
	resp_req->resp_all_mapping.node_id = server_instance->server_id;

	reval = dhmp_memory_register(dev->pd, &local_mr, 1024*1024);
	if (reval)
	{
		ERROR_LOG("dhmp_memory_register");
		exit(-1);
	}

	// 镜像节点只需要一个 mr 即可
	memcpy(&(resp_req->resp_all_mapping.mirror_mr), local_mr.mr, sizeof(struct ibv_mr));
	resp_req->resp_all_mapping.mirror_virtual_addr = local_mr.addr;

	resp->req_ptr  = req->req_ptr;		    		// 原样返回对方用于消息完成时的报文的判别
	resp->resp_ptr = resp;							// 自身用于消息完成时报文的判别
	resp->node_id  = server_instance->server_id;	// 向对端发送自己的 node_id 用于身份辨识
	resp->info_type = MICA_GET_CLIMR_RESPONSE;
	resp->info_length = resp_len;
	resp->done_flag = false;						// request_handler 不关心 done_flag（不需要阻塞）

	return resp;
}


static void 
dhmp_mica_get_cli_MR_response_handler(struct dhmp_transport* rdma_trans,
													struct dhmp_msg* msg)
{
	struct post_datagram *resp = (struct post_datagram *) (msg->data); 
	struct post_datagram *req = (struct post_datagram *) (resp->req_ptr); 

	struct dhmp_mica_get_cli_MR_response *resp_info = \
			(struct dhmp_mica_get_cli_MR_response *) DATA_ADDR(resp, 0);

	struct dhmp_mica_get_cli_MR_request *req_info = \
		(struct dhmp_mica_get_cli_MR_request *) DATA_ADDR(req, 0);

	memcpy(req_info->info_revoke_ptr, &resp_info->resp_all_mapping, sizeof(struct replica_mappings));

	INFO_LOG("Node [%d] get mr info from node[%d]!, req_info->info_revoke_ptr is [%p]",\
		 server_instance->server_id, resp->node_id, req_info->info_revoke_ptr);

	//dump_mr(&req_info->info_revoke_ptr->mrs[0]);
	//dump_mr(&req_info->info_revoke_ptr->mrs[1]);

	resp->req_ptr->done_flag = true;
}

static struct post_datagram * 
dhmp_ack_request_handler(struct dhmp_transport* rdma_trans,
							struct post_datagram *req)
{
	// 我们不拷贝直接复用 request post_datagram
	struct post_datagram *resp;
	struct dhmp_mica_ack_request * req_data = (struct dhmp_mica_ack_request *) DATA_ADDR(req, 0);
	struct dhmp_mica_ack_response * resp_data = (struct dhmp_mica_ack_response *) req_data;
	enum ack_info_state resp_ack_state;

	switch (req_data->ack_type)
	{
	case MICA_INIT_ADDR_ACK:
		mehcached_shm_lock();
		if (get_table_init_state() == false || replica_is_ready == false)
			resp_ack_state = MICA_ACK_INIT_ADDR_NOT_OK;
		else
			resp_ack_state = MICA_ACK_INIT_ADDR_OK;

		if (IS_REPLICA(server_instance->server_type) &&
			!IS_TAIL(server_instance->server_type) && 
			nic_thread_ready == false)
			resp_ack_state = MICA_ACK_INIT_ADDR_NOT_OK;

		mehcached_shm_unlock();
		break;
	default:
		break;
	}

	resp = req;											
	resp->node_id	  = server_instance->server_id;
	resp->resp_ptr    = resp;
	resp->info_type   = MICA_ACK_RESPONSE;
	resp->info_length = sizeof(struct dhmp_mica_ack_response);

	// 虽然不可见，但是 ack request 的 post_datagram 结构体
	// 的尾部有 dhmp_mica_ack_request 结构体，我们同样复用它
	resp_data->ack_state = resp_ack_state;
	INFO_LOG("resp->req_ptr  is %p, resp is %p, resp_ack_state is %d", resp->req_ptr, resp, resp_ack_state);
	return resp;
}

static void 
dhmp_ack_response_handler(struct dhmp_transport* rdma_trans,
													struct dhmp_msg* msg)
{
	struct post_datagram *resp = (struct post_datagram *) (msg->data); 
	struct post_datagram *req  = (struct post_datagram *) resp->req_ptr;

	struct dhmp_mica_ack_request * req_info = \
			(struct dhmp_mica_ack_request *) DATA_ADDR(req, 0);
	struct dhmp_mica_ack_response *resp_info = \
			(struct dhmp_mica_ack_response *) DATA_ADDR(resp, 0);

	// 直接复用 dhmp_mica_ack_request 结构体放置返回值
	req_info->ack_type = resp_info->ack_state;

	req->done_flag = true;
	INFO_LOG("resp is %p, resp->req_ptr  is %p, req is %p, resp_info->ack_state is %d", resp, resp->req_ptr , req, resp_info->ack_state);
}


static struct post_datagram * 
dhmp_node_id_request_handler(struct post_datagram *req)
{
	// 直接复用结构体
	struct post_datagram *resp;
	struct dhmp_get_nodeID_response * resp_data = \
			(struct dhmp_get_nodeID_response *) DATA_ADDR(req, 0);

	resp = req;											
	resp->node_id	  = server_instance->server_id;
	resp->resp_ptr    = resp;
	resp->info_type   = MICA_SERVER_GET_CLINET_NODE_ID_RESPONSE;
	resp->info_length = sizeof(struct dhmp_get_nodeID_response);

	resp_data->resp_node_id = server_instance->server_id;

	return resp;
}

static void 
dhmp_node_id_response_handler(struct dhmp_transport* rdma_trans,
										struct dhmp_msg* msg)
{
	struct post_datagram *resp = (struct post_datagram *) (msg->data); 
	struct post_datagram *req = resp->req_ptr; 

	struct dhmp_get_nodeID_response *resp_info = \
			(struct dhmp_get_nodeID_response *) DATA_ADDR(resp, 0);

	struct dhmp_get_nodeID_request *req_info = \
			(struct dhmp_get_nodeID_request *) DATA_ADDR(req, 0);

	req_info->node_id = resp_info->resp_node_id;

	resp->req_ptr->done_flag = true;
}

static struct post_datagram * 
dhmp_mica_get_request_handler(struct post_datagram *req)
{
	struct post_datagram *resp;
	struct dhmp_mica_get_request  * req_info;
	struct dhmp_mica_get_response * set_result;
	size_t resp_len;
	bool re;
	MICA_GET_STATUS get_status;

	void * key_addr;
	void * value_addr;

	req_info  = (struct dhmp_mica_get_request *) DATA_ADDR(req, 0);
	key_addr = (void*)req_info->data;

	if (IS_REPLICA(server_instance->server_type))
		Assert(replica_is_ready == true);

	// 在调用 get 之前还不能确定需要返回的报文长度的大小
	// 但是处于简单和避免两次RPC交互，我们默认value的长度为1k
	resp_len = sizeof(struct dhmp_mica_get_response) + req_info->peer_max_recv_buff_length;
	resp = (struct post_datagram *) malloc(DATAGRAM_ALL_LEN(resp_len));
	memset(resp, 0, DATAGRAM_ALL_LEN(resp_len));
	set_result = (struct dhmp_mica_get_response *) DATA_ADDR(resp, 0);
	value_addr = (void*)set_result + offsetof(struct dhmp_mica_get_response, out_value);

	struct mehcached_table *table = &table_o;
	uint8_t *out_value = value_addr;
	size_t in_out_value_length ;
	uint32_t out_expire_time;

	in_out_value_length = (size_t)MICA_DEFAULT_VALUE_LEN;
	re = mehcached_get(req_info->current_alloc_id,
						table,
						req_info->key_hash,
						key_addr,
						req_info->key_length,
						out_value,
						&in_out_value_length,
						&out_expire_time,
						false, 
						false,
						&get_status);	// 我们获取带 header 和 tailer 的 value

	// TODO 增加get失败的原因：不存在还是version不一致
	if (re== false)
	{
		set_result->out_expire_time = 0;
		set_result->out_value_length = (size_t)-1;
		ERROR_LOG("Node [%d] GET key_hash [%lx] failed!", server_instance->server_id, req_info->key_hash);
	}
	else
	{
		set_result->out_expire_time = out_expire_time;
		set_result->out_value_length = in_out_value_length;
		INFO_LOG("Node [%d] GET key_hash [%lx], tag is [%ld] success!, get value is %u", server_instance->server_id, req_info->key_hash, req_info->tag, *(size_t*) &(set_result->out_value[0]));
	}

	set_result->status = get_status;
	set_result->partition_id = req_info->partition_id;

	// 填充 response 报文
	resp->req_ptr  = req->req_ptr;		    		// 原样返回对方用于消息完成时的报文的判别
	resp->resp_ptr = resp;							// 自身用于消息完成时报文的判别
	resp->node_id  = server_instance->server_id;	// 向对端发送自己的 node_id 用于身份辨识
	resp->info_type = MICA_GET_RESPONSE;
	resp->info_length = resp_len;
	resp->done_flag = false;						// request_handler 不关心 done_flag（不需要阻塞）

	return resp;
}

static void 
dhmp_get_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	struct post_datagram *resp = (struct post_datagram *) (msg->data); 
	struct post_datagram *req = resp->req_ptr; 
	struct dhmp_mica_get_response * get_re_info;

	struct dhmp_mica_get_response *resp_info = \
			(struct dhmp_mica_get_response *) DATA_ADDR(resp, 0);

	struct dhmp_mica_get_request *req_info = \
			(struct dhmp_mica_get_request *) DATA_ADDR(req, 0);

	get_re_info 					= req_info->get_resp;
	get_re_info->out_value_length   = resp_info->out_value_length;
	get_re_info->out_expire_time    = resp_info->out_expire_time;
	get_re_info->status 			= resp_info->status;

	get_re_info->trans_data.msg = msg;
	get_re_info->trans_data.rdma_trans = rdma_trans;

	if (resp_info->out_value_length != (size_t) -1)
	{
		void * value_addr =  (void*)(resp_info->out_value);
		// memcpy((void*)(get_re_info->out_value), value_addr, resp_info->out_value_length );
		get_re_info->msg_buff_addr = value_addr;

		INFO_LOG("Node [%d] GET key_hash [%lx] from node[%d]!, is success!, tag is [%ld]", \
			server_instance!=NULL ? server_instance->server_id : client_mgr->self_node_id, req_info->key_hash, resp->node_id, req_info->tag);
	}
	else
	{
		ERROR_LOG("Node [%d] GET key_hash [%lx] from node[%d]!, is FAILED!, tag is [%ld]", \
			server_instance!=NULL ? server_instance->server_id : client_mgr->self_node_id, req_info->key_hash, resp->node_id, req_info->tag);
	}

	resp->req_ptr->done_flag = true;
}

static struct post_datagram * 
dhmp_mica_update_notify_request_handler(struct post_datagram *req)
{
	Assert(IS_REPLICA(server_instance->server_type));
	struct post_datagram *resp=NULL;
	struct dhmp_update_notify_request  * req_info;
	// struct dhmp_update_notify_response * set_result;
	// size_t resp_len = sizeof(struct dhmp_update_notify_response);

	// 访问 value 各部分的基址
	struct mehcached_item * update_item;
	struct mehcached_table *table = &table_o;
    struct midd_value_header* value_base;
    // uint8_t* value_data;
    // struct midd_value_tail* value_tail;
	size_t key_align_length;
	size_t value_len, true_value_len;
	uint64_t item_offset;

	if (IS_REPLICA(server_instance->server_type))
		Assert(replica_is_ready == true);

	req_info  = (struct dhmp_update_notify_request *) DATA_ADDR(req, 0);
	update_item = get_item_by_offset(table, req_info->item_offset);

#ifdef INFO_DEBUG
	WARN_LOG("[dhmp_mica_update_notify_request_handler] get tag [%ld]", req_info->tag);
#endif
	key_align_length = MEHCACHED_ROUNDUP8(MEHCACHED_KEY_LENGTH(update_item->kv_length_vec));
	value_len = MEHCACHED_VALUE_LENGTH(update_item->kv_length_vec);
	true_value_len = value_len - VALUE_HEADER_LEN - VALUE_TAIL_LEN;

	item_offset = get_offset_by_item(table, update_item);
	Assert(item_offset == req_info->item_offset);

    value_base = (struct midd_value_header*)(update_item->data + key_align_length);
    // value_data = update_item->data + key_align_length + VALUE_HEADER_LEN;
    // value_tail = (struct midd_value_tail*) VALUE_TAIL_ADDR(update_item->data , key_align_length, true_value_len);

#ifdef INFO_DEBUG
	WARN_LOG("Node [%d] recv update value, now value version is [%ld]", \
				server_instance->server_id, value_base->version);
#endif
	// 不用等待数据传输完成，直接启动下一次传输
	/*
		DEFINE_STACK_TIMER();
		MICA_TIME_COUNTER_INIT();
		while (memcmp(&value_base->version, &value_tail->version, sizeof(uint64_t)) != 0)
			MICA_TIME_LIMITED(req_info->tag, 150);
		MICA_TIME_COUNTER_CAL("dhmp_mica_update_notify_request_handler:wait value_base->version==value_tail->version !");

		MICA_TIME_COUNTER_INIT();
		while (value_tail->dirty == true)
			MICA_TIME_LIMITED(req_info->tag, 150);
		MICA_TIME_COUNTER_CAL("dhmp_mica_update_notify_request_handler: wait value_tail->dirty is false");

		WARN_LOG("Node [%d] finished update value, now value version is [%ld]", \
					server_instance->server_id, value_base->version);
	*/

	// 在收到上游节点传递来的value后，继续向下游节点传播
	if (!IS_TAIL(server_instance->server_type))
	{
		// 生成 nic 任务下放到网卡发送链表
        makeup_update_request(update_item, item_offset, (uint8_t *)value_base, value_len, req_info->tag);
		INFO_LOG("Node [%d] continue send value to downstream node", server_instance->server_id);
	}

/*
	resp_len = sizeof(struct dhmp_update_notify_response);
	resp = (struct post_datagram *) malloc(DATAGRAM_ALL_LEN(resp_len));
	set_result = (struct dhmp_update_notify_response *) DATA_ADDR(resp, 0);
	set_result->is_success = true;
	set_result->tag = req_info->tag;
	set_result->partition_id = req_info->partition_id;

	// 填充 response 报文
	resp->req_ptr  = req->req_ptr;		    		// 原样返回对方用于消息完成时的报文的判别
	resp->resp_ptr = resp;							// 自身用于消息完成时报文的判别
	resp->node_id  = server_instance->server_id;	// 向对端发送自己的 node_id 用于身份辨识
	resp->info_type = MICA_REPLICA_UPDATE_RESPONSE;
	resp->info_length = resp_len;
	resp->done_flag = false;						// request_handler 不关心 done_flag（不需要阻塞）
*/
	return resp;
}

static void 
dhmp_mica_update_notify_response_handler(struct dhmp_msg* msg)
{
	struct post_datagram *resp = (struct post_datagram *) (msg->data); 
	struct post_datagram *req = resp->req_ptr; 
	struct dhmp_update_notify_response *resp_info = \
			(struct dhmp_update_notify_response *) DATA_ADDR(resp, 0);

	WARN_LOG("[dhmp_mica_update_notify_response_handler] get tag [%d]", resp_info->tag);
	INFO_LOG("Node [%d] get update response!" , server_instance->server_id);
	resp->req_ptr->done_flag = true;
}

// 我们只让 server 端按照分区进行多线程处理，客户端不会多线程化。 
static void dhmp_send_request_handler(struct dhmp_transport* rdma_trans,
											struct dhmp_msg* msg, bool * is_async, __time_t time_start1, __syscall_slong_t time_start2)
{
	struct post_datagram *req = (struct post_datagram*)msg->data;
	struct timespec end;
	switch (req->info_type)
	{
		case MICA_ACK_REQUEST:
		case MICA_GET_CLIMR_REQUEST:
		case MICA_SERVER_GET_CLINET_NODE_ID_REQUEST:
		case MICA_REPLICA_UPDATE_REQUEST:
			// 非分区的操作就在主线程执行（可能的性能问题，也许需要一个专门的线程负责处理非分区的操作，但是非分区操作一般是初始化操作，对后续性能影响不明显）
			__dhmp_send_request_handler(rdma_trans, msg);
			*is_async = false;
			break;
		case MICA_SET_REQUEST:
		case MICA_GET_REQUEST:
		case MICA_SET_REQUEST_TEST:
			// 分区的操作需要分布到特定的线程去执行
			clock_gettime(CLOCK_MONOTONIC, &end);
			//INFO_LOG("Distribute [%ld] ns", (((end.tv_sec * 1000000000) + end.tv_nsec) - ((time_start1 * 1000000000) + time_start2)));
			distribute_partition_resp(get_req_partition_id(req), rdma_trans, msg, DHMP_MICA_SEND_INFO_REQUEST, end.tv_sec ,end.tv_nsec);
			*is_async = true;
			break;
		default:
			ERROR_LOG("Unknown request info_type %d", req->info_type);
			exit(0);
			break;
	}

	return ;
}

// 所有的双边 rdma 操作 request_handler 的路由入口
// 这个函数只暴露基本的数据报 post_datagram 结构体，不涉及具体的数据报内容
// 根据 info_type 去调用正确的回调函数对数据报进行处理。 
static void __dhmp_send_request_handler(struct dhmp_transport* rdma_trans,
											struct dhmp_msg* msg)
{
	struct dhmp_msg res_msg;
	//struct dhmp_device * dev;
	struct post_datagram *req;
	struct post_datagram *resp;
	struct timespec start, end; 

	req = (struct post_datagram*)msg->data;

	// if (server_instance == NULL)
	// 	dev = dhmp_get_dev_from_client();
	// else
	// 	dev = dhmp_get_dev_from_server();

#ifdef MAIN_LOG_DEBUG_THROUGHOUT
	if (set_counts == 100)
		clock_gettime(CLOCK_MONOTONIC, &start_through);

	if (set_counts == 1100)
	{
		clock_gettime(CLOCK_MONOTONIC, &end_through);
		total_set_through_time = ((((end_through.tv_sec * 1000000000) + end_through.tv_nsec) - ((start_through.tv_sec * 1000000000) + start_through.tv_nsec)));
		ERROR_LOG("[dhmp_mica_set_request_handler] count[%d] through_out time is [%lld] us", 1000, total_set_through_time /1000);
	}
#endif

	switch (req->info_type)
	{
		case MICA_ACK_REQUEST:
			INFO_LOG ( "Recv [MICA_ACK_REQUEST] from node [%d]",  req->node_id);	
			resp = dhmp_ack_request_handler(rdma_trans, req);
			break;	
		case MICA_GET_CLIMR_REQUEST:
			INFO_LOG ( "Recv [MICA_GET_CLIMR_REQUEST] from node [%d]",  req->node_id);	
			resp = dhmp_mica_get_MR_request_handler(rdma_trans, req);
			break;
		case MICA_SERVER_GET_CLINET_NODE_ID_REQUEST:
			INFO_LOG ( "Recv [MICA_SERVER_GET_CLINET_NODE_ID_REQUEST] from node [%d]",  req->node_id);
			resp = dhmp_node_id_request_handler(req);
			break;
		case MICA_SET_REQUEST:
			//INFO_LOG ( "Recv [MICA_SET_REQUEST] from node [%d]",  req->node_id);
			set_counts++;	// 有并发问题，这个值只是为了debug
			clock_gettime(CLOCK_MONOTONIC, &start);	
			// resp = dhmp_mica_set_request_handler(req);
			dhmp_mica_set_request_handler(rdma_trans, req);
			clock_gettime(CLOCK_MONOTONIC, &end);	
			// if (set_counts >=100)
			//total_set_time = (((end.tv_sec * 1000000000) + end.tv_nsec) - ((start.tv_sec * 1000000000) + start.tv_nsec));
			//INFO_LOG("All set [%ld] us",total_set_time/1000 );
			// if (server_instance->server_id == 0 && set_counts == 1000)
			// 	ERROR_LOG("[dhmp_mica_set_request_handler] count[%d] avg time is [%lld]us", set_counts, total_set_time / (US_BASE*(set_counts-100)));
			return ;
		case MICA_GET_REQUEST:
			//INFO_LOG ( "Recv [MICA_GET_REQUEST] from node [%d]",  req->node_id);
			get_counts++;	// 有并发问题，这个值只是为了debug
			clock_gettime(CLOCK_MONOTONIC, &start);	
			resp = dhmp_mica_get_request_handler(req);
			clock_gettime(CLOCK_MONOTONIC, &end);	
			if (set_counts >=100)
				total_get_time += (((end.tv_sec * 1000000000) + end.tv_nsec) - ((start.tv_sec * 1000000000) + start.tv_nsec));
			if (get_counts==1000)
				ERROR_LOG("[dhmp_mica_get_request_handler] count[%d] avg time is [%lld]ns", get_counts, total_get_time / ((NS_BASE-100)*(get_counts-100)));
			break;
		case MICA_REPLICA_UPDATE_REQUEST:
			//INFO_LOG ( "Recv [MICA_REPLICA_UPDATE_REQUEST] from node [%d]",  req->node_id);
			// 我们不需要再向上游 nic 发送确认，直接启动向下游节点 nic 的数据传输
			// resp = dhmp_mica_update_notify_request_handler(req);
			dhmp_mica_update_notify_request_handler(req);
			return;
		case MICA_SET_REQUEST_TEST:
			// do nothing
			resp = (struct post_datagram *) malloc(sizeof(struct post_datagram ));
			memset(resp, 0, sizeof(struct post_datagram ));
			resp->info_type = MICA_SET_RESPONSE_TEST;
			resp->node_id = MAIN;
			resp->req_ptr = req->req_ptr;
			resp->info_length = 0;
			break;
		case MICA_GET_P2P_MR_REQUEST:

			break;
		default:
			ERROR_LOG("Unknown request info_type %d", req->info_type);
			exit(0);
			break;
	}

	res_msg.msg_type = DHMP_MICA_SEND_INFO_RESPONSE;
	res_msg.data_size = DATAGRAM_ALL_LEN(resp->info_length);
	res_msg.data= resp;
	//INFO_LOG("Send response msg length is [%u] KB", res_msg.data_size / 1024);

	// 向主节点返回，将主节点从阻塞状态中返回
	clock_gettime(CLOCK_MONOTONIC, &start);	
	dhmp_post_send(rdma_trans, &res_msg);
	clock_gettime(CLOCK_MONOTONIC, &end);
	total_get_time = (((end.tv_sec * 1000000000) + end.tv_nsec) - ((start.tv_sec * 1000000000) + start.tv_nsec));
	ERROR_LOG("[dhmp_post_send]   time is [%lld] ns", total_get_time);
send_clean:
	// 需要注意的是，因为我们直接复用了 request ack 报文，所以 req->info_type 变成了
	// resp 的类型，所以下面的 if 判断是用 req 的指针判断了 response 的消息
	if (req->info_type != MICA_ACK_RESPONSE &&
	    req->info_type != MICA_SERVER_GET_CLINET_NODE_ID_RESPONSE)
		free((void*) resp);

	return ;
}

static void 
dhmp_send_respone_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, bool *is_async, __time_t time_start1, __syscall_slong_t time_start2)
{
	struct post_datagram *req = (struct post_datagram*)msg->data;
	struct timespec end;
	// 我们只让 server 端按照分区进行多线程处理，客户端不会多线程化。 
	// 但是有一个特殊情况，为了避免额外的拷贝，我们会让 mica 客户端的 get 操作异步化，数据会直接从 “双边操作缓冲区”
	// 直接拷贝到 “用户目标地址”， 免除在 response 函数中的额外的拷贝
	if (server_instance == NULL)
	{
		if (req->info_type == MICA_GET_RESPONSE)
			*is_async = true;	// 这里为true的含义是不让 rw 处理线程释放掉 recv_mr 区域
		else
			*is_async = false;
		__dhmp_send_respone_handler(rdma_trans, msg);
		return;
	}

	switch (req->info_type)
	{
		case MICA_ACK_RESPONSE:
		case MICA_GET_CLIMR_RESPONSE:
		case MICA_SERVER_GET_CLINET_NODE_ID_RESPONSE:
		case MICA_REPLICA_UPDATE_RESPONSE:

		case MICA_GET_RESPONSE:
		case MICA_SET_RESPONSE:
		case MICA_SET_RESPONSE_TEST:
			// 非分区的操作就在主线程执行（可能的性能问题，也许需要一个专门的线程负责处理非分区的操作，但是非分区操作一般是初始化操作，对后续性能影响不明显）
			__dhmp_send_respone_handler(rdma_trans, msg);
			*is_async = false;
			break;

			// // 分区的操作需要分布到特定的线程去执行
			// clock_gettime(CLOCK_MONOTONIC, &end);
			// //INFO_LOG("RESP Distribute [%ld] ns", (((end.tv_sec * 1000000000) + end.tv_nsec) - ((time_start1 * 1000000000) + time_start2)));
			// distribute_partition_resp(get_resp_partition_id(req), rdma_trans, msg, DHMP_MICA_SEND_INFO_RESPONSE, end.tv_sec ,end.tv_nsec);
			// *is_async = true;
			// break;
		default:
			ERROR_LOG("Unknown request info_type %d", req->info_type);
			Assert(0);
			break;
	}

	return ;
}

// 在RPC中，我们一般可以阻塞 send 操作，但是千万不要阻塞 recv 操作
static void 
__dhmp_send_respone_handler(struct dhmp_transport* rdma_trans,
													struct dhmp_msg* msg)
{
	struct post_datagram *resp;
	resp = (struct post_datagram*)msg->data;
	DEFINE_STACK_TIMER();

	switch (resp->info_type)
	{
		case MICA_ACK_RESPONSE:
			INFO_LOG ( "Recv [MICA_ACK_RESPONSE] from node [%d]",  resp->node_id);
			dhmp_ack_response_handler(rdma_trans, msg);
			break;	
		case MICA_GET_CLIMR_RESPONSE:
			INFO_LOG ( "Recv [MICA_GET_CLIMR_RESPONSE] from node [%d]",  resp->node_id);
			dhmp_mica_get_cli_MR_response_handler(rdma_trans, msg);
			break;
		case MICA_SERVER_GET_CLINET_NODE_ID_RESPONSE:
			INFO_LOG ( "Recv [MICA_SERVER_GET_CLINET_NODE_ID_RESPONSE] from node [%d]",  resp->node_id);
			dhmp_node_id_response_handler(rdma_trans, msg);
			break;
		case MICA_SET_RESPONSE:
			//INFO_LOG ( "Recv [MICA_SET_RESPONSE] from node [%d]",  resp->node_id);
			//MICA_TIME_COUNTER_INIT();
			dhmp_set_response_handler(msg);
			//MICA_TIME_COUNTER_CAL("dhmp_set_response_handler");
			break;
		case MICA_GET_RESPONSE:
			//INFO_LOG ( "Recv [MICA_GET_RESPONSE] from node [%d]",  resp->node_id);
			dhmp_get_response_handler(rdma_trans, msg); // 需要 rdma_trans 进行 recv_wr 的卸载
			break;		
		case MICA_REPLICA_UPDATE_RESPONSE:
			//INFO_LOG ( "Recv [MICA_REPLICA_UPDATE_RESPONSE] from node [%d]",  resp->node_id);
			// dhmp_mica_update_notify_response_handler(msg);
			break;				
		case MICA_SET_RESPONSE_TEST:
			((struct post_datagram *)(msg->data))->req_ptr->done_flag=true;
			break;
		default:
			break;
	}
}

/**
 *	dhmp_wc_recv_handler:handle the IBV_WC_RECV event
 *  多线程改进的 wc_recv 函数，会进行线程分发操作
 */
void dhmp_wc_recv_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, bool *is_async, __time_t time_start1, __syscall_slong_t time_start2)
{
	switch(msg->msg_type)
	{
		case DHMP_MICA_SEND_INFO_REQUEST:
			dhmp_send_request_handler(rdma_trans, msg, is_async, time_start1, time_start2);
			break;
		case DHMP_MICA_SEND_INFO_RESPONSE:
			dhmp_send_respone_handler(rdma_trans, msg, is_async, time_start1, time_start2);
			break;

		case DHMP_MSG_CLOSE_CONNECTION:
			rdma_disconnect(rdma_trans->cm_id);
			*is_async = false;
			break;
		default:
			*is_async = false;
			break;
	}
}

// 原始的 wc_recv 函数，不会执行线程分发操作
void __dhmp_wc_recv_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	switch(msg->msg_type)
	{
		case DHMP_MICA_SEND_INFO_REQUEST:
			__dhmp_send_request_handler(rdma_trans, msg);
			break;
		case DHMP_MICA_SEND_INFO_RESPONSE:
			__dhmp_send_respone_handler(rdma_trans, msg);
			break;
		case DHMP_MSG_CLOSE_CONNECTION:
			rdma_disconnect(rdma_trans->cm_id);
			break;
		default:
			break;
	}
}

static struct dhmp_msg *
make_basic_msg(struct dhmp_msg * res_msg, struct post_datagram *resp)
{
	res_msg->msg_type = DHMP_MICA_SEND_INFO_RESPONSE;
	res_msg->data_size = DATAGRAM_ALL_LEN(resp->info_length);
	res_msg->data= resp;
	return res_msg;
}

	// item = mehcached_set(req_info->current_alloc_id,
	// 					table,
	// 					req_info->key_hash,
	// 					key_addr,
	// 					req_info->key_length,
	// 					value_addr,
	// 					req_info->value_length,
	// 					req_info->expire_time,
	// 					req_info->overwrite,
	// 					&is_update,
	// 					&is_maintable,
	// 					NULL);


void 
main_node_broadcast_matedata(struct dhmp_mica_set_request  * req_info, 
							  struct mehcached_item * item, void * key_addr, 
							  void * value_addr, bool is_update, bool is_maintable)
{
    int nid, reval;
	struct set_requset_pack req_callback_ptr[10];	// 默认不超过10个节点
	//DEFINE_STACK_TIMER();
	struct timespec start, end, start_g, end_g, start_l, end_l;
	long long time1,time2,time3, time4=0;

    // 镜像节点全value赋值
	// 我们这里也不使用 warpper 函数，因为已经加上了元数据的长度
	//MICA_TIME_COUNTER_INIT();
	// clock_gettime(CLOCK_MONOTONIC, &start_g);
	// clock_gettime(CLOCK_MONOTONIC, &start);
    for (nid = MIRROR_NODE_ID; nid <= REPLICA_NODE_TAIL_ID; nid++)
    {
		if (nid == MIRROR_NODE_ID)
		{
			clock_gettime(CLOCK_MONOTONIC, &start);
			
			// dhmp_rdma_write_mica_warpper(find_connect_server_by_nodeID(MIRROR_NODE_ID),
			// 							item,
			// 							&mirror_node_mapping->mirror_mr,
			// 							req_info->value_length,
			// 							mirror_node_mapping->mirror_virtual_addr,
			// 							true);
			
			struct dhmp_task write_task;
			struct ibv_sge sge;
			struct ibv_send_wr send_wr,*bad_wr=NULL;
			struct dhmp_transport* trans = find_connect_server_by_nodeID(MIRROR_NODE_ID);
			struct ibv_mr *local_mr;

			if (is_maintable)
				local_mr = return_shm_mr(table_mapping_id_1);
			else
				local_mr = return_shm_mr(table_mapping_id_2);

			write_task.done_flag = false;
			write_task.is_imm = true;
			mirror_node_mapping->in_used_flag = true;

			memset(&send_wr, 0, sizeof(struct ibv_send_wr));
			send_wr.opcode=IBV_WR_RDMA_WRITE_WITH_IMM;
			send_wr.imm_data   = htonl(0x1234);
			send_wr.wr_id= ( uintptr_t ) &write_task;
			send_wr.sg_list=&sge;
			send_wr.num_sge=1;
			send_wr.send_flags=IBV_SEND_SIGNALED;
			send_wr.wr.rdma.remote_addr= mirror_node_mapping->mirror_virtual_addr;  // WGT
			send_wr.wr.rdma.rkey=mirror_node_mapping->mirror_mr.rkey;

			sge.addr=	trans->send_mr.addr;// ( uintptr_t ) item_get_value_addr(item);
			sge.length=	req_info->value_length;
			sge.lkey=	trans->send_mr.mr->lkey;
			reval=ibv_post_send ( trans->qp, &send_wr, &bad_wr );
			if ( reval )
			{
				ERROR_LOG("ibv_post_send error");
				exit(-1);
			}
			while (!write_task.done_flag);

			clock_gettime(CLOCK_MONOTONIC, &end);
			time1 = (((end.tv_sec * 1000000000) + end.tv_nsec) - ((start.tv_sec * 1000000000) + start.tv_nsec)); 
			INFO_LOG("RDMA imm [%ld]", time1);
		}
		else
		{
			//MICA_TIME_COUNTER_INIT();
			mica_set_remote(req_info->current_alloc_id, req_info->key_hash, 
							key_addr, 
							req_info->key_length, 
							value_addr, 
							req_info->value_length,
							0, true, true, 
							&req_callback_ptr[nid], 
							nid, 
							is_update, 
							server_instance->server_id,
							req_info->tag);
			//MICA_TIME_COUNTER_CAL("[dhmp_mica_set_request_handler]->[broadcast]->[for:req_callback_ptr]->[mica_set_remote]");
		}
    }
	//MICA_TIME_COUNTER_CAL("[dhmp_mica_set_request_handler]->[broadcast]->[for:1]");
	// clock_gettime(CLOCK_MONOTONIC, &end);			    	
	// time1 = (((end.tv_sec * 1000000000) + end.tv_nsec) - ((start.tv_sec * 1000000000) + start.tv_nsec)); 
	
	//MICA_TIME_COUNTER_INIT();
    // for (nid = MIRROR_NODE_ID; nid <= REPLICA_NODE_TAIL_ID; nid++)
	// clock_gettime(CLOCK_MONOTONIC, &start);
	for (nid = MIRROR_NODE_ID; nid <= REPLICA_NODE_TAIL_ID; nid++)
    {
		if (nid == MIRROR_NODE_ID)
		{
			
			MICA_TIME_COUNTER_INIT();
			clock_gettime(CLOCK_MONOTONIC, &start);
			while(mirror_node_mapping->in_used_flag== true);
				//MICA_TIME_LIMITED(req_info->tag, TIMEOUT_LIMIT_MS);
			clock_gettime(CLOCK_MONOTONIC, &end);
			time1 = (((end.tv_sec * 1000000000) + end.tv_nsec) - ((start.tv_sec * 1000000000) + start.tv_nsec)); 
			INFO_LOG("RDMA imm done flag [%ld]", time1);
			MICA_TIME_COUNTER_CAL("[dhmp_mica_set_request_handler]->[broadcast]->[for:req_callback_ptr]->[mirror]");
		}
		else
		{
			MICA_TIME_COUNTER_INIT();
			clock_gettime(CLOCK_MONOTONIC, &start_l);
			while(req_callback_ptr[nid].req_ptr->done_flag == false)
				MICA_TIME_LIMITED(req_info->tag, TIMEOUT_LIMIT_MS);
			clock_gettime(CLOCK_MONOTONIC, &end_l);
			time4 += (((end_l.tv_sec * 1000000000) + end_l.tv_nsec) - ((start_l.tv_sec * 1000000000) + start_l.tv_nsec)); 
			INFO_LOG("RDMA send-recv done flag [%ld]", time4);
			//MICA_TIME_COUNTER_CAL("[dhmp_mica_set_request_handler]->[broadcast]->[for:req_callback_ptr]->[replica]");

			if (req_callback_ptr[nid].req_info_ptr->out_mapping_id == (size_t)-1)
			{
				ERROR_LOG("Main node set node[%d] key_hash [%lx] failed!", nid, req_callback_ptr[nid].req_info_ptr->key_hash);
				exit(0);
			}
			else
			{
				// 主节点只需要保存直接下游节点的 mr 信息即可
				if (nid == REPLICA_NODE_HEAD_ID)
				{
					item->mapping_id = req_callback_ptr[nid].req_info_ptr->out_mapping_id;
					item->remote_value_addr = req_callback_ptr[nid].req_info_ptr->out_value_addr;
					//INFO_LOG("Main node set node[%d] key_hash [%lx] success!, mapping id is %u, remote addr is %p", \
									nid, item->key_hash, item->mapping_id, item->remote_value_addr);
				}
			}

			free(req_callback_ptr[nid].req_ptr);
		}
    }
	// clock_gettime(CLOCK_MONOTONIC, &end);	
	// clock_gettime(CLOCK_MONOTONIC, &end_g);		    	
	// time2 = (((end.tv_sec * 1000000000) + end.tv_nsec) - ((start.tv_sec * 1000000000) + start.tv_nsec)); 
	// time3 = (((end_g.tv_sec * 1000000000) + end_g.tv_nsec) - ((start_g.tv_sec * 1000000000) + start_g.tv_nsec)); 
	INFO_LOG("RTT : %ld us", time4);
	// INFO_LOG("time1:%ld us, time2:%ld us, time4: %ld us, time3:%ld us", time1/1000, time2/1000, time4/1000, time3/1000);
	//INFO_LOG("[%s] exec time is [%lld] us", "[dhmp_mica_set_request_handler]->[broadcast]->[for:1]", mica_total_time_ns / US_BASE); 
	// MICA_TIME_COUNTER_CAL("[dhmp_mica_set_request_handler]->[broadcast]->[for:2]");
    // INFO_LOG("Main Node, key hash [%lx] set to all replica node success!", req_info->key_hash);
}

int init_mulit_server_work_thread()
{
	
	bool recv_mulit_threads_enable=true;
	int i, j, retval;
	cpu_set_t cpuset;
	mica_work_context_mgr[0] = (struct mica_work_context *) malloc(sizeof(struct mica_work_context));
	mica_work_context_mgr[1] = (struct mica_work_context *) malloc(sizeof(struct mica_work_context));
	memset(mica_work_context_mgr[0], 0, sizeof(struct mica_work_context));
	memset(mica_work_context_mgr[1], 0, sizeof(struct mica_work_context));

	for (j=DHMP_MICA_SEND_INFO_REQUEST; j<=DHMP_MICA_SEND_INFO_REQUEST; j++)
	{
		for (i=0; i<PARTITION_NUMS; i++)
		{
			// CPU_ZERO(&cpuset);
			// CPU_SET(i, &cpuset);
			thread_init_data *data = (thread_init_data *) malloc(sizeof(thread_init_data));
			data->partition_id = i;
			data->thread_type = (enum dhmp_msg_type) j;

			retval=pthread_create(&(mica_work_context_mgr[j-DHMP_MICA_SEND_INFO_REQUEST]->threads[i]), NULL, mica_work_thread, (void*)data);
			if(retval)
			{
				ERROR_LOG("pthread create error.");
				return -1;
			}
			// 绑核
			// retval = pthread_setaffinity_np(mica_work_context_mgr->threads[i], sizeof(cpu_set_t), &cpuset);
			// if (retval != 0)
			// 	handle_error_en(retval, "pthread_setaffinity_np");
			// INFO_LOG("set affinity cpu [%d] to thread [%d]", i, i);
		}
	}
}

int  get_req_partition_id(struct post_datagram *req)
{
	switch (req->info_type)
	{
		case MICA_SET_REQUEST:
			return ((struct dhmp_mica_set_request *) DATA_ADDR(req, 0))->partition_id; 
		case MICA_GET_REQUEST:
			return ((struct dhmp_mica_get_request *) DATA_ADDR(req, 0))->partition_id; 
		case MICA_REPLICA_UPDATE_REQUEST:
			return ((struct dhmp_update_notify_request *) DATA_ADDR(req, 0))->partition_id;
		case MICA_SET_REQUEST_TEST:
			return 0;
		default:
			ERROR_LOG("Unsupport partition request info_type %d", req->info_type);
			Assert(false);
			break;
	}
}

int  get_resp_partition_id(struct post_datagram *req)
{
	switch (req->info_type)
	{
		case MICA_SET_RESPONSE:
			return ((struct dhmp_mica_set_response *) DATA_ADDR(req, 0))->partition_id; 
		case MICA_GET_RESPONSE:
			return ((struct dhmp_mica_get_response  *) DATA_ADDR(req, 0))->partition_id; 
		case MICA_REPLICA_UPDATE_RESPONSE:
			return ((struct dhmp_update_notify_response *) DATA_ADDR(req, 0))->partition_id;
		default:
			ERROR_LOG("Unsupport partition response info_type %d", req->info_type);
			Assert(false);
			break;
	}
}

void distribute_partition_resp(int partition_id, struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, enum dhmp_msg_type type, __time_t time_start1, __syscall_slong_t time_start2)
{
	struct mica_work_context * mgr;
	volatile uint64_t * lock;
	struct post_datagram * req = (struct post_datagram*)msg->data;
	struct timespec  start_g, end_g, start_l, end_l;
	long long time1,time2,time3, time4=0;

	switch (type)
	{
		case DHMP_MICA_SEND_INFO_REQUEST:
			mgr = mica_work_context_mgr[0];
			break;
		case DHMP_MICA_SEND_INFO_RESPONSE:
			mgr = mica_work_context_mgr[1];
			break;
		default:
			ERROR_LOG("Unkown type");;
			Assert(false);
			break;
	}
	lock = &(mgr->bit_locks[partition_id]);

	DEFINE_STACK_TIMER();
	MICA_TIME_COUNTER_INIT();

	if (__partition_nums == 1)
	{
		// 执行分区的操作
		// __dhmp_send_request_handler(trans_msg.rdma_trans, trans_msg.msg);
		__dhmp_wc_recv_handler(rdma_trans, msg);
		
		clock_gettime(CLOCK_MONOTONIC, &end_g);	
		time2 = (((end_g.tv_sec * 1000000000) + end_g.tv_nsec) - ((time_start1 * 1000000000) + time_start2)); 
		INFO_LOG("__dhmp_wc_recv_handler time2: [%ld] us", time2/1000);
		//MICA_TIME_COUNTER_CAL("__dhmp_wc_recv_handler!");

		// 回收发送缓冲区
		// 发送双边操作的数据大小不能超过  SINGLE_NORM_RECV_REGION （16MB）
		dhmp_post_recv(rdma_trans, msg->data - sizeof(enum dhmp_msg_type) - sizeof(size_t));

		//MICA_TIME_COUNTER_CAL("dhmp_post_recv!");

		// #define container_of(ptr, type, member)
		// 防止 msg 内存泄漏
		free(container_of(&(msg->data), struct dhmp_msg , data));
	}
	else
	{
		if (mgr->buff_msg_data[partition_id].set_tag == '1')
		{
			while(true)
			{
				//clock_gettime(CLOCK_MONOTONIC, &start_g);
				partition_lock(lock);
				// 必须要等待下游线程处理完buffer中的消息后才能放置新的消息
				// memory_barrier();
				if(mgr->buff_msg_data[partition_id].set_tag == '1')
					partition_unlock(lock);
				else
					break;
			}
		}

		//memory_barrier();
		mgr->buff_msg_data[partition_id].msg = msg;
		mgr->buff_msg_data[partition_id].rdma_trans = rdma_trans;
		mgr->buff_msg_data[partition_id].resp_type = req->info_type;
		// 最后设置标记位
		memory_barrier();
		mgr->buff_msg_data[partition_id].set_tag = (volatile char)'1';

		partition_unlock(lock);
		clock_gettime(CLOCK_MONOTONIC, &end_g);	
		time2 = (((end_g.tv_sec * 1000000000) + end_g.tv_nsec) - ((time_start1 * 1000000000) + time_start2)); 
		//INFO_LOG("distribute_partition_resp time2: [%ld] us", time2/1000);
		//MICA_TIME_COUNTER_CAL("distribute_partition_resp");
		//INFO_LOG("distribute msg [%d]!", partition_id);
	}
}

void* mica_work_thread(void *data)
{
	int partition_id;
	enum dhmp_msg_type type;
	volatile uint64_t * lock;
	thread_init_data * init_data = (thread_init_data*) data;
	struct dhmp_msg* msg = NULL;
	struct mica_work_context * mgr;
	struct timespec start_g, end_g, start_l, end_l;
	long long time1,time2,time3, time4=0;

	partition_id = init_data->partition_id;
	type = init_data->thread_type;

	switch (type)
	{
		case DHMP_MICA_SEND_INFO_REQUEST:
			mgr = mica_work_context_mgr[0];
			break;
		case DHMP_MICA_SEND_INFO_RESPONSE:
			mgr = mica_work_context_mgr[1];
			break;
		default:
			break;
	}

	lock = &(mgr->bit_locks[partition_id]);
	free(init_data);

	INFO_LOG("Server work thread [%d] launch!", partition_id);
	while (true)
	{
		// memory_barrier();
		if (mgr->buff_msg_data[partition_id].set_tag == '1')
		{
			struct dhmp_mica_msg_data trans_msg;
			DEFINE_STACK_TIMER();

			clock_gettime(CLOCK_MONOTONIC, &start_g);
			MICA_TIME_COUNTER_INIT();
			partition_lock(lock);
			memcpy(&trans_msg, &(mgr->buff_msg_data[partition_id]), sizeof(struct dhmp_mica_msg_data)); 
			// memset(&(mgr->buff_msg_data[partition_id]), 0, sizeof(struct dhmp_mica_msg_data));

			// 如果写入完成标志位不是由一个 memcpy 完成的，那么需要用 barrier 保证写的顺序
			memory_barrier();
			mgr->buff_msg_data[partition_id].set_tag = (volatile char)0;
			partition_unlock(lock);

			clock_gettime(CLOCK_MONOTONIC, &end_g);	
			time2 = (((end_g.tv_sec * 1000000000) + end_g.tv_nsec) - ((start_g.tv_sec * 1000000000) + start_g.tv_nsec)); 
			//INFO_LOG("mica_work_thread lock time2:%ld us", time2/1000);
	
			//MICA_TIME_COUNTER_CAL("partition_unlock!");

			// INFO_LOG("server thread [%d] get info type:[%d]!", partition_id, type);

			// 执行分区的操作
			// __dhmp_send_request_handler(trans_msg.rdma_trans, trans_msg.msg);
			__dhmp_wc_recv_handler(trans_msg.rdma_trans, trans_msg.msg);

			//MICA_TIME_COUNTER_CAL("__dhmp_wc_recv_handler!");

			// 回收发送缓冲区
			// 发送双边操作的数据大小不能超过  SINGLE_NORM_RECV_REGION （16MB）
			dhmp_post_recv(trans_msg.rdma_trans, trans_msg.msg->data - sizeof(enum dhmp_msg_type) - sizeof(size_t));

			//MICA_TIME_COUNTER_CAL("dhmp_post_recv!");

			// #define container_of(ptr, type, member)
			// 防止 msg 内存泄漏
			free(container_of(&(trans_msg.msg->data), struct dhmp_msg , data));
			//MICA_TIME_COUNTER_CAL("mica_req_work_thread");
		}
	}
}


static struct post_datagram * 
dhmp_mica_get_MR_request_handler(struct dhmp_transport* rdma_trans,
									struct post_datagram *req)
{
	if (IS_MIRROR(server_instance->server_type))
		return dhmp_mica_get_Mirr_MR_request_handler(rdma_trans, req);
	else if (IS_REPLICA(server_instance->server_type))
		return dhmp_mica_get_cli_MR_request_handler(rdma_trans, req);
	else
	{
		ERROR_LOG("dhmp_mica_get_MR_request_handler");
		exit(-1);
	}
}

#define MAIN_LOG_DEBUG
void
dhmp_mica_main_replica_set_request_handler(struct dhmp_transport* rdma_trans, struct post_datagram *req)
{
	DEFINE_STACK_TIMER();
	MICA_TIME_COUNTER_INIT();
	struct timespec start_g, end_g;
	clock_gettime(CLOCK_MONOTONIC, &start_g);	
	struct post_datagram *resp;
	struct dhmp_mica_set_request  * req_info;
	struct dhmp_mica_set_response * set_result;
	size_t resp_len = sizeof(struct dhmp_mica_set_response);
	struct mehcached_item * item;
	struct mehcached_table *table = &table_o;
	bool is_update, is_maintable = true;
	struct dhmp_msg resp_msg;
	struct dhmp_msg *resp_msg_ptr;

	void * key_addr;
	void * value_addr;

	// 时间戳
	struct timespec time_set = {0, 0};

	if (IS_REPLICA(server_instance->server_type))
		Assert(replica_is_ready == true);

	req_info  = (struct dhmp_mica_set_request *) DATA_ADDR(req, 0);
	key_addr = (void*)req_info->data;	

	// mica客户端节点向主节点发送请求也会调用这个函数
	// Assert(!IS_MAIN(server_instance->server_type));
	// 不管是 insert 还是 update 副本节点都需要等待上游网卡节点传送数据
	if (IS_REPLICA(server_instance->server_type))
		value_addr = (void*) 0x1;
	else
		value_addr = (void*)key_addr + req_info->key_length;

	// 该节点的 mapping 信息和 mr 信息
	// 回传key（为了上游节点确定item，仅靠key_hash是不够的）
	if (!IS_HEAD(server_instance->server_type) &&
		!IS_MIRROR(server_instance->server_type) &&
		!IS_MAIN(server_instance->server_type))
		resp_len += req_info->key_length;	

	resp = (struct post_datagram *) malloc(DATAGRAM_ALL_LEN(resp_len));
	memset(resp, 0, DATAGRAM_ALL_LEN(resp_len));
	set_result = (struct dhmp_mica_set_response *) DATA_ADDR(resp, 0);

#ifdef MAIN_LOG_DEBUG
	if (IS_MAIN(server_instance->server_type))
	{
		//MICA_TIME_COUNTER_CAL("[dhmp_mica_set_request_handler]->[init]")
		//WARN_LOG("Get set tag[%ld]", req_info->tag);
	}
#endif
	// 注意！，此处不需要调用 warpper 函数
	// 因为传递过来的 value 地址已经添加好头部和尾部了，长度也已经加上了头部和尾部的长度。
	
	MICA_TIME_COUNTER_INIT();
	item = mehcached_set(req_info->current_alloc_id,
						table,
						req_info->key_hash,
						key_addr,
						req_info->key_length,
						value_addr,
						req_info->value_length,
						req_info->expire_time,
						req_info->overwrite,
						&is_update,
						&is_maintable,
						NULL);
#ifdef MAIN_LOG_DEBUG
	//if (IS_MAIN(server_instance->server_type))
	//	MICA_TIME_COUNTER_CAL("[dhmp_mica_set_request_handler]->[mehcached_set]")
#endif
	// Assert(is_update == req_info->is_update);
	if (IS_REPLICA(server_instance->server_type))
		Assert(is_maintable == true);

	set_result->partition_id = req_info->partition_id;
	if (item != NULL)
	{
		// 从item中获取的value地址是包括value元数据的
		set_result->value_addr = (uintptr_t ) item_get_value_addr(item);
		set_result->out_mapping_id = item->mapping_id;
		set_result->is_success = true;
		// INFO_LOG("MICA node [%d] get set request, set key_hash is \"%lx\",  mapping id is [%u] , value addr is [%p]", \
						server_instance->server_id, req_info->key_hash, set_result->out_mapping_id, set_result->value_addr);
	}
	else
	{
		ERROR_LOG("MICA node [%d] get set request, set key_hash is \"%lx\", set key FAIL!", \
				server_instance->server_id, req_info->key_hash);
		set_result->out_mapping_id = (size_t) - 1;
		set_result->is_success = false;
		Assert(false);
	}

	if (IS_MAIN(server_instance->server_type))
	{
		// void 
		// main_node_broadcast_matedata(struct dhmp_mica_set_request  * req_info, 
		// 							struct mehcached_item * item, void * key_addr, uint64_t key_hash,  
		// 							void * value_addr, bool is_update, bool is_maintable)
		// 现在有一个问题需要确定，传递过来的 key 和 value 应该是不携带元数据的，但是此时 key 和 value 的长度已经加上了元数据的长度
		
		MICA_TIME_COUNTER_INIT();
		main_node_broadcast_matedata(req_info,
									item,
									key_addr,
									value_addr,
									is_update,
									is_maintable);
#ifdef MAIN_LOG_DEBUG
	//	MICA_TIME_COUNTER_CAL("[dhmp_mica_set_request_handler]->[broadcast]");
	//	INFO_LOG("Main Node: key hash [%lx] notices downstream replica node!", req_info->key_hash); 
#endif
	}
	clock_gettime(CLOCK_MONOTONIC, &end_g);	

#ifdef MAIN_LOG_DEBUG_LATENCE
	if (set_counts >=100)
		total_set_latency_time += ((((end_g.tv_sec * 1000000000) + end_g.tv_nsec) - ((start_g.tv_sec * 1000000000) + start_g.tv_nsec)));
	if (server_instance->server_id == 0 && set_counts>200)
		ERROR_LOG("[dhmp_mica_set_request_handler] count[%d] avg time is [%lld]us", set_counts, total_set_latency_time / (US_BASE*(set_counts-100)));
#endif

	// 拷贝 key 和 key 的长度,用于链中节点向上游节点发送消息
	if (!IS_HEAD(server_instance->server_type) &&
		!IS_MIRROR(server_instance->server_type) &&
		!IS_MAIN(server_instance->server_type))
	{
		memcpy(set_result->key_data, key_addr, req_info->key_length);
		set_result->key_length = req_info->key_length;
		set_result->key_hash = req_info->key_hash;
	}

	// 填充 response 报文
	resp->req_ptr  = req->req_ptr;		    		// 原样返回对方用于消息完成时的报文的判别
	resp->resp_ptr = resp;							// 自身用于消息完成时报文的判别
	resp->node_id  = server_instance->server_id;	// 向对端发送自己的 node_id 用于身份辨识
	resp->info_type = MICA_SET_RESPONSE;
	resp->info_length = resp_len;
	resp->done_flag = false;						// request_handler 不关心 done_flag（不需要阻塞）

	// 副本节点先向 主节点 发送回复，节约一次 RTT 的时间
	// 主节点向客户端发送回复
	MICA_TIME_COUNTER_INIT();
	resp_msg_ptr = make_basic_msg(&resp_msg, resp);
	dhmp_post_send(rdma_trans, resp_msg_ptr);

#ifdef MAIN_LOG_DEBUG
	//if (IS_MAIN(server_instance->server_type))
	//	MICA_TIME_COUNTER_CAL("[dhmp_mica_set_request_handler]->[dhmp_post_send]");
#endif
	// 各副本节点（除了主节点直接负责的副本节点）还需要向各自的直接上游
	// 节点发送自己的新分配的 item 的 mappingID 和虚拟地址
	if (!IS_HEAD(server_instance->server_type) &&
		!IS_MIRROR(server_instance->server_type) &&
		!IS_MAIN(server_instance->server_type) )
	{
		// 发送给上游节点，我们是被动建立连接的一方，是服务端
		// MICA_TIME_COUNTER_INIT();
		dhmp_post_send(find_connect_client_by_nodeID(server_instance->server_id - 1), resp_msg_ptr);
		// MICA_TIME_COUNTER_CAL("[dhmp_mica_set_request_handler]->[dhmp_post_send to upstream node]");
	}

	if (IS_MAIN(server_instance->server_type))
	{
		size_t item_offset;
		
		if (is_maintable)
			item_offset = get_offset_by_item(main_table, item);
		else
			item_offset = get_offset_by_item(log_table, item);

		// 只写直接下游节点
		// 还需要返回远端 value 的虚拟地址， 用来求偏移量
		MICA_TIME_COUNTER_INIT();
		makeup_update_request(item, item_offset,\
								(uint8_t*)item_get_value_addr(item), \
								MEHCACHED_VALUE_LENGTH(item->kv_length_vec),\
								req_info->tag);
#ifdef MAIN_LOG_DEBUG
	//	MICA_TIME_COUNTER_CAL("[dhmp_mica_set_request_handler]->[makeup_update_request]");
#endif
	}

	// INFO_LOG("key_hash is %lx, len is %lu, addr is %p ", req_info->key_hash, req_info->key_length, key_addr);
}

static void 
dhmp_mica_mirror_set_request_handler(struct dhmp_transport* rdma_trans)
{
	struct post_datagram *resp;
	struct dhmp_mica_set_response * set_result;
	size_t resp_len = sizeof(struct dhmp_mica_set_response);
	struct mehcached_item * item;
	struct mehcached_table *table = &table_o;
	bool is_update, is_maintable = true;
	struct dhmp_msg resp_msg;
	struct dhmp_msg *resp_msg_ptr;

	resp = (struct post_datagram *) malloc(DATAGRAM_ALL_LEN(resp_len));
	memset(resp, 0, DATAGRAM_ALL_LEN(resp_len));
	set_result = (struct dhmp_mica_set_response *) DATA_ADDR(resp, 0);
	set_result->is_success = true;
	
	// 填充 response 报文
	resp->req_ptr  = NULL;		    		// 原样返回对方用于消息完成时的报文的判别
	resp->resp_ptr = resp;							// 自身用于消息完成时报文的判别
	resp->node_id  = server_instance->server_id;	// 向对端发送自己的 node_id 用于身份辨识
	resp->info_type = MICA_SET_RESPONSE;
	resp->info_length = resp_len;
	resp->done_flag = false;						// request_handler 不关心 done_flag（不需要阻塞）

	resp_msg_ptr = make_basic_msg(&resp_msg, resp);
	dhmp_post_send(rdma_trans, resp_msg_ptr);

#ifdef INFO_DEBUG
	INFO_LOG("[dhmp_mica_mirror_set_request_handler] is ok");
#endif
}

void
dhmp_mica_set_request_handler(struct dhmp_transport* rdma_trans, struct post_datagram *req)
{
	if (IS_MAIN(server_instance->server_type) || 
		IS_REPLICA(server_instance->server_type))
		dhmp_mica_main_replica_set_request_handler(rdma_trans, req);
	else if (IS_MIRROR(server_instance->server_type))
		dhmp_mica_mirror_set_request_handler(rdma_trans);
	else
	{
		ERROR_LOG("dhmp_mica_set_request_handler");
		exit(-1);
	}
}

static void
dhmp_set_client_response_handler(struct post_datagram *resp, 
									struct post_datagram *req, 
									struct dhmp_mica_set_response *resp_info, 
									struct dhmp_mica_set_request  *req_info)
{
	req_info->out_mapping_id = resp_info->out_mapping_id;
	req_info->out_value_addr = resp_info->value_addr;
	req_info->is_success = resp_info->is_success;		
	resp->req_ptr->done_flag = true;
}

static void
dhmp_set_server_response_handler(struct post_datagram *resp, 
									struct post_datagram *req, 
									struct dhmp_mica_set_response *resp_info, 
									struct dhmp_mica_set_request  *req_info)
{
	DEFINE_STACK_TIMER();
	
	if (IS_MAIN(server_instance->server_type))
	{
		if (resp->node_id == MIRROR_NODE_ID)
		{
			//INFO_LOG("Main node get Mirror node set response");
			// 主节点相应 MIRROT节点 set response，注意 req_info 是无意义的，因为主节点没有发送 dhmp_mica_set_request
			mirror_node_mapping->in_used_flag = false;
			// 主节点只需要获得一个 ack ，除此之外不需要从镜像节点获得任何信息
			return;
		}
		else
		{
			// 主节点相应副本节点 set response
			//INFO_LOG("Main node get repilice node set response");
			req_info->out_mapping_id = resp_info->out_mapping_id;
			req_info->out_value_addr = resp_info->value_addr;
			req_info->is_success = resp_info->is_success;		
			resp->req_ptr->done_flag = true;
			return;
		}
	}
	else if (IS_REPLICA(server_instance->server_type))
	{
		Assert(replica_is_ready == true);
	
		// 由于上游副本节点没有发布 dhmp_mica_set_request， 所以 resp->req_ptr 指针
		// 没有意义，不要去试图访问 req_ptr 指针
		uint64_t key_hash = resp_info->key_hash;
		size_t key_length = resp_info->key_length;
		uint8_t * key_addr = (uint8_t*)resp_info->key_data;
		struct mehcached_item * target_item;
		struct mehcached_table *table = &table_o; // 副本节点只有一个 table

		// 这个时刻一定要保证 hash 表已经初始化完成了
		// 根据 key_hash 查表，将下游节点的 out_mapping_id 和 out_value_addr 赋值
		// 有可能下游节点已经向上游节点返回set信息，但是主节点的set信息元数据还没有到达，因此需要while等待
		//INFO_LOG("key_hash is %lx, len is %lu, addr is %p ", key_hash, key_length, key_addr);
		MICA_TIME_COUNTER_INIT();
		while (true)
		{
			target_item = find_item(table, key_hash , key_addr, key_length);
			if (target_item != NULL)
				break;
			MICA_TIME_LIMITED(req_info->tag, 50);
		}

		target_item->mapping_id = resp_info->out_mapping_id;
		target_item->remote_value_addr = resp_info->value_addr;
		//INFO_LOG("Node [%d] recv downstram node's key_hash \"[%lx]\"'s virtual addr info is %p", \
						server_instance->server_id, resp_info->key_hash, target_item->remote_value_addr);
		return;
	}
}

static void 
dhmp_set_response_handler(struct dhmp_msg* msg)
{
	struct post_datagram *resp = (struct post_datagram *) (msg->data); 
	struct post_datagram *req  = resp->req_ptr; 
	struct dhmp_mica_set_response *resp_info = (struct dhmp_mica_set_response *) DATA_ADDR(resp, 0);
	struct dhmp_mica_set_request  *req_info = (struct dhmp_mica_set_request *) DATA_ADDR(req, 0);

	if (server_instance == NULL)
		dhmp_set_client_response_handler(resp, req, resp_info, req_info);
	else
		dhmp_set_server_response_handler(resp, req, resp_info, req_info);

	// INFO_LOG("Node [%d] recv set resp from node[%d]!, is success!", \
			server_instance!=NULL ? server_instance->server_id : client_mgr->self_node_id,resp->node_id);
}

static void 
dhmp_get_p2p_buff(struct dhmp_transport* rdma_trans, struct post_datagram *req)
{
	struct post_datagram *resp;
	struct dhmp_mica_get_p2p_MR_info_RQ  * req_info;
	struct dhmp_mica_get_p2p_MR_info_RQ  * resp_req;
	struct dhmp_device * dev = dhmp_get_dev_from_server();
	size_t resp_len = sizeof(struct dhmp_mica_get_p2p_MR_info_RQ);

	// Mirror arg
	struct dhmp_mr local_mr;
	int reval;

	resp = (struct post_datagram *) malloc(DATAGRAM_ALL_LEN(resp_len));
	INFO_LOG("resp length is %u", DATAGRAM_ALL_LEN(resp_len));
	memset(resp, 0, DATAGRAM_ALL_LEN(resp_len));

	req_info  = (struct dhmp_mica_get_p2p_MR_info_RQ *) DATA_ADDR(req, 0);
	resp_req  = (struct dhmp_mica_get_p2p_MR_info_RQ *) DATA_ADDR(resp, 0);

	// 填充自身新产生的信息
	

	resp->req_ptr  = req->req_ptr;		    		// 原样返回对方用于消息完成时的报文的判别
	resp->resp_ptr = resp;							// 自身用于消息完成时报文的判别
	resp->node_id  = server_instance->server_id;	// 向对端发送自己的 node_id 用于身份辨识
	resp->info_type = MICA_GET_CLIMR_RESPONSE;
	resp->info_length = resp_len;
	resp->done_flag = false;						// request_handler 不关心 done_flag（不需要阻塞）

	return resp;
}