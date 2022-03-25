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
#include "dhmp.h"

#include "dhmp_top_api.h"
#include "midd_mica_benchmark.h"

//#define MAIN_LOG_DEBUG_LATENCE
#define MAIN_LOG_DEBUG_THROUGHOUT 
struct timespec start_through, end_through;

volatile bool replica_is_ready = true;
uint64_t set_counts = 0, get_counts=0, op_counts=0;
long long int total_set_time = 0, total_get_time =0;
long long int total_set_latency_time = 0, total_set_through_time=0;
long long int total_through_time = 0;

// 主线程和 partition 线程互相同步的变量
struct mica_work_context mica_work_context_mgr[2];
unsigned long long  __partition_nums;
int partition_set_count[PARTITION_MAX_NUMS];

struct list_head partition_local_send_list[PARTITION_MAX_NUMS];   
struct list_head main_thread_send_list[PARTITION_MAX_NUMS];
uint64_t partition_work_nums[PARTITION_MAX_NUMS];

static struct dhmp_msg * make_basic_msg(struct dhmp_msg * res_msg, struct post_datagram *resp, enum dhmp_msg_type type);
static void  dhmp_send_respone_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, bool *is_async);
void dhmp_send_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, bool * is_async);

// 由于 send 操作可能会被阻塞住，所以必须将 recv 操作让另一个线程处理，否则会出现死锁。
// 我们对每一个 partition 启动两个线程
void* mica_work_thread(void *data);

int get_req_partition_id(struct post_datagram *req);
int get_resp_partition_id(struct post_datagram *req);
void distribute_partition_resp(int partition_id, struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);

static void __dhmp_send_respone_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, size_t partition_id);
static void __dhmp_send_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, size_t partition_id, bool * need_post_recv);

void dhmp_mica_set_request_handler(struct dhmp_transport* rdma_trans, struct post_datagram *req, int partition_id);

/*
	本地读
*/
struct dhmp_msg** get_msgs_group;
static void  check_get_op(struct dhmp_msg* msg, size_t partition_id);


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
			mehcached_table_init(table, TABLE_BUCKET_NUMS, 1, TABLE_POOL_SIZE, false, false, false, \
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

	dump_mr(&resp_req->resp_all_mapping.mrs[1]);

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

	dump_mr(&req_info->info_revoke_ptr->mrs[0]);
	dump_mr(&req_info->info_revoke_ptr->mrs[1]);

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

static void 
dhmp_set_response_handler(struct dhmp_msg* msg)
{
	struct post_datagram *resp = (struct post_datagram *) (msg->data); 
	struct dhmp_mica_set_response *resp_info = \
			(struct dhmp_mica_set_response *) DATA_ADDR(resp, 0);
	struct post_datagram *req = resp->req_ptr; 
	struct dhmp_mica_set_request *req_info = \
			(struct dhmp_mica_set_request *) DATA_ADDR(req, 0);
	
	DEFINE_STACK_TIMER();

	req_info->out_mapping_id = resp_info->out_mapping_id;
	req_info->out_value_addr = resp_info->value_addr;
	req_info->is_success = resp_info->is_success;		// 新增的成功标识位，如果失败需要重试

	resp->req_ptr->done_flag = true;

	//INFO_LOG("Node [%d] set key_hash [%lx], tag is [%d] to node[%d]!, is success!", \
			server_instance!=NULL ? server_instance->server_id : client_mgr->self_node_id, req_info->key_hash, req_info->tag ,resp->node_id);
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
	
	// 增加一次 memcpy
	memmove(out_value, out_value, req_info->peer_max_recv_buff_length);

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
		//ERROR_LOG("Node [%d] GET key_hash [%lx] failed!", server_instance->server_id, req_info->key_hash);
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

	//ERROR_LOG("Get: tag is [%d] partition_id [%d]",req_info->tag, req_info->partition_id);
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
	Assert(false);
	return NULL;
}

static void 
dhmp_mica_update_notify_response_handler(struct dhmp_msg* msg)
{
	Assert(false);
	return;	
}



// 所有的双边 rdma 操作 request_handler 的路由入口
// 这个函数只暴露基本的数据报 post_datagram 结构体，不涉及具体的数据报内容
// 根据 info_type 去调用正确的回调函数对数据报进行处理。 
static void __dhmp_send_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, size_t partition_id, bool * need_post_recv)
{
	struct dhmp_msg res_msg;
	struct dhmp_device * dev;
	struct post_datagram *req;
	struct post_datagram *resp;
	struct timespec start, end; 

	req = (struct post_datagram*)msg->data;
	*need_post_recv = true;

	switch (req->info_type)
	{
		case MICA_ACK_REQUEST:
			INFO_LOG ( "Recv [MICA_ACK_REQUEST] from node [%d]",  req->node_id);	
			resp = dhmp_ack_request_handler(rdma_trans, req);
			break;	
		case MICA_GET_CLIMR_REQUEST:
			INFO_LOG ( "Recv [MICA_GET_CLIMR_REQUEST] from node [%d]",  req->node_id);	
			resp = dhmp_mica_get_cli_MR_request_handler(rdma_trans, req);
			break;
		case MICA_SERVER_GET_CLINET_NODE_ID_REQUEST:
			INFO_LOG ( "Recv [MICA_SERVER_GET_CLINET_NODE_ID_REQUEST] from node [%d]",  req->node_id);
			resp = dhmp_node_id_request_handler(req);
			break;
		case MICA_SET_REQUEST:
			//INFO_LOG ( "Recv [MICA_SET_REQUEST] from node [%d]",  req->node_id);
			dhmp_mica_set_request_handler(rdma_trans, req, partition_id);

			// 主/头 节点的写操作是本地生成的
			if (server_instance->server_id == 0)
				*need_post_recv = false;
			return; 	// 直接返回
		case MICA_GET_REQUEST:
			// INFO_LOG ( "Recv [MICA_GET_REQUEST] from node [%d]",  req->node_id);
			// get_counts++;	// 有并发问题，这个值只是为了debug
			// clock_gettime(CLOCK_MONOTONIC, &start);	
			dhmp_mica_get_request_handler(req);
			// clock_gettime(CLOCK_MONOTONIC, &end);	
			// total_get_time += (((end.tv_sec * 1000000000) + end.tv_nsec) - ((start.tv_sec * 1000000000) + start.tv_nsec));
			// if (get_counts==1024)
			// 	WARN_LOG("[dhmp_mica_get_request_handler] count[%d] avg time is [%lld]ns", get_counts, total_get_time / (NS_BASE*get_counts));

			// 目前所有的读都是本地读，是本地生成的读请求
			*need_post_recv = false;
			return;  // 直接返回
		default:
			ERROR_LOG("Unknown request info_type %d", req->info_type);
			exit(0);
			break;
	}

	res_msg.msg_type = DHMP_MICA_SEND_INFO_RESPONSE;
	res_msg.data_size = DATAGRAM_ALL_LEN(resp->info_length);
	res_msg.data= resp;
	// INFO_LOG("Send response msg length is [%u] KB", res_msg.data_size / 1024);

	// 向主节点返回，将主节点从阻塞状态中返回
	dhmp_post_send(rdma_trans, &res_msg, partition_id);

send_clean:
	// 需要注意的是，因为我们直接复用了 request ack 报文，所以 req->info_type 变成了
	// resp 的类型，所以下面的 if 判断是用 req 的指针判断了 response 的消息
	if (req->info_type != MICA_ACK_RESPONSE &&
	    req->info_type != MICA_SERVER_GET_CLINET_NODE_ID_RESPONSE)
		free((void*) resp);

	return ;
}

// 在RPC中，我们一般可以阻塞 send 操作，但是千万不要阻塞 recv 操作
static void 
__dhmp_send_respone_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, size_t partition_id)
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
			Assert(false);
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
			INFO_LOG ( "Recv [MICA_GET_RESPONSE] from node [%d]",  resp->node_id);
			dhmp_get_response_handler(rdma_trans, msg); // 需要 rdma_trans 进行 recv_wr 的卸载
			break;		
		case MICA_REPLICA_UPDATE_RESPONSE:
			Assert(false);
			break;				
		default:
			break;
	}
}

/**
 *	dhmp_wc_recv_handler:handle the IBV_WC_RECV event
 *  函数前缀没有双下划线的函数是单线程执行的
 */
void dhmp_wc_recv_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, bool *is_async)
{
	switch(msg->msg_type)
	{
		case DHMP_MICA_SEND_INFO_REQUEST:
			dhmp_send_request_handler(rdma_trans, msg, is_async);
			break;
		case DHMP_MICA_SEND_INFO_RESPONSE:
			dhmp_send_respone_handler(rdma_trans, msg, is_async);
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

// 多线程执行
void __dhmp_wc_recv_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, size_t partition_id, bool *need_post_recv)
{
	switch(msg->msg_type)
	{
		case DHMP_MICA_SEND_INFO_REQUEST:
			__dhmp_send_request_handler(rdma_trans, msg, partition_id, need_post_recv);
			break;
		case DHMP_MICA_SEND_INFO_RESPONSE:
			*need_post_recv = true;
			__dhmp_send_respone_handler(rdma_trans, msg, PARTITION_NUMS);
			break;
		case DHMP_MSG_CLOSE_CONNECTION:
			ERROR_LOG("DHMP_MSG_CLOSE_CONNECTION! exit");
			exit(-1);
			rdma_disconnect(rdma_trans->cm_id);
			break;
		default:
			break;
	}
}

static struct dhmp_msg *
make_basic_msg(struct dhmp_msg * res_msg, struct post_datagram *resp, enum dhmp_msg_type type)
{
	res_msg->msg_type = type;
	res_msg->data_size = DATAGRAM_ALL_LEN(resp->info_length);
	res_msg->data= resp;
	return res_msg;
}

int init_mulit_server_work_thread()
{
	
	bool recv_mulit_threads_enable=true;
	int i, retval;
	cpu_set_t cpuset;
	memset(&(mica_work_context_mgr[0]), 0, sizeof(struct mica_work_context));
	memset(&(mica_work_context_mgr[1]), 0, sizeof(struct mica_work_context));

	
	for (i=0; i<PARTITION_NUMS; i++)
	{
		// CPU_ZERO(&cpuset);
		// CPU_SET(i, &cpuset);
		thread_init_data *data = (thread_init_data *) malloc(sizeof(thread_init_data));
		data->partition_id = i;
		data->thread_type = (enum dhmp_msg_type) DHMP_MICA_SEND_INFO_REQUEST;

		INIT_LIST_HEAD(&main_thread_send_list[i]);
		INIT_LIST_HEAD(&partition_local_send_list[i]);

		retval=pthread_create(&(mica_work_context_mgr[DHMP_MICA_SEND_INFO_REQUEST].threads[i]), NULL, mica_work_thread, (void*)data);
		if(retval)
		{
			ERROR_LOG("pthread create error.");
			return -1;
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

void distribute_partition_resp(int partition_id, struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	struct mica_work_context * mgr;
	volatile uint64_t * lock;
	struct post_datagram * req = (struct post_datagram*)(msg->data);
	struct timespec  start_g, end_g, start_l, end_l;
	long long time1,time2,time3, time4=0;

	switch (msg->msg_type)
	{
		case DHMP_MICA_SEND_INFO_REQUEST:
			mgr = &mica_work_context_mgr[0];
			break;
		case DHMP_MICA_SEND_INFO_RESPONSE:
			mgr = &mica_work_context_mgr[1];
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
		bool need_post_recv = true;
		__dhmp_wc_recv_handler(rdma_trans, msg, PARTITION_NUMS, &need_post_recv);

		// 间歇性的执行 get操作，暂时中断当前的写操作
		if (msg->main_thread_set_id != -1)
			check_get_op(msg, partition_id);

		if (need_post_recv)
		{
			// 回收发送缓冲区, 发送双边操作的数据大小不能超过  SINGLE_NORM_RECV_REGION （16MB）
			dhmp_post_recv(rdma_trans, msg->data - sizeof(enum dhmp_msg_type) - sizeof(size_t), msg->recv_partition_id);
			free(container_of(&(msg->data), struct dhmp_msg , data));
		}
	}
	else
	{
		// 如果去掉速度限制，则链表空指针的断言会失败，应该还是有线程间同步bug
		for (;;)
		{
			if (partition_work_nums[partition_id] <= 20)
				break;
		}
		// while(partition_work_nums[partition_id] != 0);

		partition_lock(lock);
		//Assert(msg->list_anchor.next != LIST_POISON1 && msg->list_anchor.prev!= LIST_POISON2);
		list_add(&msg->list_anchor,  &main_thread_send_list[partition_id]);
		partition_work_nums[partition_id]++;
		//Assert(msg->list_anchor.next != LIST_POISON1 && msg->list_anchor.prev!= LIST_POISON2);
		partition_unlock(lock);

	}
}

void* mica_work_thread(void *data)
{
	int partition_id, i;
	enum dhmp_msg_type type;
	volatile uint64_t * lock;
	thread_init_data * init_data = (thread_init_data*) data;
	struct mica_work_context * mgr;
	struct timespec start_g, end_g, start_l, end_l;
	long long time1,time2,time3, time4=0;
	struct dhmp_msg* msg=NULL, *temp_msg=NULL;
	int msg_nums =0;

	partition_id = init_data->partition_id;
	type = init_data->thread_type;
	partition_set_count[partition_id] = 0;
	switch (type)
	{
		case DHMP_MICA_SEND_INFO_REQUEST:
			mgr = &mica_work_context_mgr[0];
			break;
		case DHMP_MICA_SEND_INFO_RESPONSE:
			mgr = &mica_work_context_mgr[1];
			break;
		default:
			break;
	}

	lock = &(mgr->bit_locks[partition_id]);
	volatile char * flag = &(mgr->buff_msg_data[partition_id].set_tag);
	free(init_data);

	INFO_LOG("Server work thread [%d] launch!", partition_id);
	while (true)
	{
		//memory_barrier();
		if (partition_work_nums[partition_id] != 0)
		{
			partition_lock(lock);
			list_replace(&main_thread_send_list[partition_id], &partition_local_send_list[partition_id]); 
			INIT_LIST_HEAD(&main_thread_send_list[partition_id]);   
			partition_work_nums[partition_id] = 0;
			partition_unlock(lock);

			list_for_each_entry_safe(msg, temp_msg, &(partition_local_send_list[partition_id]), list_anchor)
			{
				// 执行分区的操作
				Assert(msg->list_anchor.next != LIST_POISON1 && msg->list_anchor.prev!= LIST_POISON2);
				bool need_post_recv = true;

				__dhmp_wc_recv_handler(msg->trans, msg, partition_id, &need_post_recv);
				list_del(&msg->list_anchor);

				if (need_post_recv)
				{
					dhmp_post_recv(msg->trans, msg->data - sizeof(enum dhmp_msg_type) - sizeof(size_t), msg->recv_partition_id);
					free(container_of(&(msg->data), struct dhmp_msg , data));
				}

				// 间歇性的执行 get操作，暂时中断当前的写操作
				if (msg->main_thread_set_id != -1)
					check_get_op(msg, partition_id);
			}
			// 将本地头节点重置为空
			INIT_LIST_HEAD(&partition_local_send_list[partition_id]);
		}
	}
}

#define MAIN_LOG_DEBUG
void
dhmp_mica_set_request_handler(struct dhmp_transport* rdma_trans, struct post_datagram *req, int partition_id)
{
	struct timespec start_g, end_g;
	clock_gettime(CLOCK_MONOTONIC, &start_g);	
	struct post_datagram *resp, *old_req_datagram;
	struct dhmp_mica_set_request  * req_info;
	struct dhmp_mica_set_response * set_result;
	size_t resp_len = sizeof(struct dhmp_mica_set_response);
	struct mehcached_item * item;
	struct mehcached_table *table = &table_o;
	bool is_update, is_maintable = true;
	struct dhmp_msg resp_msg, req_msg;
	struct dhmp_msg *resp_msg_ptr, *req_msg_ptr;

	void * key_addr;
	void * true_value_addr;

	// 时间戳
	struct timespec time_set = {0, 0};
	DEFINE_STACK_TIMER();

	if (IS_REPLICA(server_instance->server_type))
		Assert(replica_is_ready == true);

	req_info  = (struct dhmp_mica_set_request *) DATA_ADDR(req, 0);
	key_addr = (void*)req_info->data;
	true_value_addr = (void*)key_addr + req_info->key_length + VALUE_HEADER_LEN;

	resp = (struct post_datagram *) malloc(DATAGRAM_ALL_LEN(resp_len));
	memset(resp, 0, DATAGRAM_ALL_LEN(resp_len));
	set_result = (struct dhmp_mica_set_response *) DATA_ADDR(resp, 0);

	//ERROR_LOG("Set: tag is [%d] partition_id [%d]",req_info->tag, partition_id);

	// 注意！，此处不需要调用 warpper 函数
	// 因为传递过来的 value 地址已经添加好头部和尾部了，长度也已经加上了头部和尾部的长度。
	MICA_TIME_COUNTER_INIT();
	item = mehcached_set(req_info->current_alloc_id,
						table,
						req_info->key_hash,
						key_addr,
						req_info->key_length,
						true_value_addr,
						req_info->value_length,
						req_info->expire_time,
						req_info->overwrite,
						&is_update,
						&is_maintable,
						NULL);
	//MICA_TIME_COUNTER_CAL("[]->[mehcached_set]")
	
	// 大概上，用set函数返回的时间作为各个 set 操作的时间顺序
	clock_gettime(CLOCK_MONOTONIC, &time_set);   
	
	if (IS_REPLICA(server_instance->server_type))
		Assert(is_maintable == true);

	set_result->partition_id = req_info->partition_id;
	if (item != NULL)
	{
		// 从item中获取的value地址是包括value元数据的
		set_result->value_addr = (uintptr_t ) item_get_value_addr(item);
		set_result->out_mapping_id = item->mapping_id;
		set_result->is_success = true;
		//INFO_LOG("MICA node [%d] get set request, set key_hash is \"%lx\",  mapping id is [%u] , value addr is [%p]", \
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

	// 向下游节点转发 set 操作， 并等待 set 操作返回
	old_req_datagram = req->req_ptr;
	req->req_ptr = req;
	req->node_id = server_instance->server_id;
	req->done_flag = false;
	req_msg_ptr = make_basic_msg(&req_msg, req, DHMP_MICA_SEND_INFO_REQUEST);

	// 上游节点主动发起连接， 下游节点是 server
	dhmp_post_send(find_connect_server_by_nodeID(server_instance->server_id + 1), req_msg_ptr, partition_id);
	if (IS_MAIN(server_instance->server_type))
	{
		MICA_TIME_COUNTER_INIT();
		while(req->done_flag == false)
			MICA_TIME_LIMITED(req_info->tag, TIMEOUT_LIMIT_MS);
		//MICA_TIME_COUNTER_CAL("Tag set downstream");
	}
	clock_gettime(CLOCK_MONOTONIC, &end_g);	

	//ERROR_LOG("tag is [%d] partition_id [%d] send 1",req_info->tag, partition_id);
#ifdef MAIN_LOG_DEBUG
	// if (set_counts >=100)
	// 	total_set_time += ((((end_g.tv_sec * 1000000000) + end_g.tv_nsec) - ((start_g.tv_sec * 1000000000) + start_g.tv_nsec)));
	// if (server_instance->server_id == 0 && set_counts>200)
	// 	ERROR_LOG("[] count[%d] avg time is [%lld]us", set_counts, total_set_time / (US_BASE*(set_counts-100)));
#endif

	// 填充 response 报文
	resp->req_ptr  = old_req_datagram;		    		// 原样返回对方用于消息完成时的报文的判别
	resp->resp_ptr = resp;							// 自身用于消息完成时报文的判别
	resp->node_id  = server_instance->server_id;	// 向对端发送自己的 node_id 用于身份辨识
	resp->info_type = MICA_SET_RESPONSE;
	resp->info_length = resp_len;
	resp->done_flag = false;						// request_handler 不关心 done_flag（不需要阻塞）

	if (server_instance->server_id !=0)
	{
		// 先向 主节点/ 客户端 发送回复，节约一次 RTT 的时间
		resp_msg_ptr = make_basic_msg(&resp_msg, resp, DHMP_MICA_SEND_INFO_RESPONSE);
		dhmp_post_send(rdma_trans, resp_msg_ptr, partition_id);
	}
	//ERROR_LOG("tag is [%d] partition_id [%d] send 2",req_info->tag, partition_id);
}

// 函数前缀没有双下划线的函数是单线程执行的
// 单线程的 handler 函数可以安全的放置全局计时和统计操作
// 我们只让 server 端按照分区进行多线程处理，客户端不会多线程化。 
void dhmp_send_request_handler(struct dhmp_transport* rdma_trans,
											struct dhmp_msg* msg, bool * is_async)
{
	int i;
	struct post_datagram *req = (struct post_datagram*)msg->data;
	bool is_get=true;

	switch (req->info_type)
	{
		case MICA_SET_REQUEST:
			set_counts++;
			is_get=false;
		case MICA_GET_REQUEST:
#ifdef THROUGH_TEST
			if (op_counts ==0)
				clock_gettime(CLOCK_MONOTONIC, &start_through);  
			
			op_counts++;
			if (server_instance->server_id == 0 && op_counts == update_num)
			{
				clock_gettime(CLOCK_MONOTONIC, &end_through); 
				total_through_time = ((((end_through.tv_sec * 1000000000) + end_through.tv_nsec) - ((start_through.tv_sec * 1000000000) + start_through.tv_nsec)));
				ERROR_LOG("set op count [%d], total op count [%d] total time is [%d] us", op_counts, __access_num, total_through_time / 1000);
			}
#endif
			if (!is_get)
				msg->main_thread_set_id = set_counts; 	// 必须在主线程设置全局唯一的 set_id

			// 不要忘记执行作为触发者的set
			distribute_partition_resp(get_req_partition_id(req), rdma_trans, msg);

			// 分区的操作需要分布到特定的线程去执行
			if (!is_get)
			{
				if (little_idx != 0)
				{
					Assert(get_is_more);
					int op_gap = op_gaps[little_idx-1];
					if (!(server_instance->server_id == 0 && !main_node_is_readable))
					{
						// get操作如果多，只能使用for循环自己触发
						int count=get_counts+op_gap;
						for(; get_counts<count; get_counts++)
						{
							Assert(get_counts <= read_num);
							get_msgs_group[get_counts]->main_thread_set_id = -1;
							distribute_partition_resp(get_msgs_group[get_counts]->partition_id, rdma_trans, get_msgs_group[get_counts]);
						}
						//ERROR_LOG("distribute [%d] get task, now get_counts is [%d]", op_gap, get_counts);
					}
				}
			}

			*is_async = true;
			break;
		default:
			ERROR_LOG("Unknown request info_type %d", req->info_type);
			exit(0);
			break;
	}

	return ;
}

static void 
dhmp_send_respone_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, bool *is_async)
{
	struct post_datagram *req = (struct post_datagram*)msg->data;

	// 我们只让 server 端按照分区进行多线程处理，客户端不会多线程化。 
	// 但是有一个特殊情况，为了避免额外的拷贝，我们会让 mica 客户端的 get 操作异步化，数据会直接从 “双边操作缓冲区”
	// 直接拷贝到 “用户目标地址”， 免除在 response 函数中的额外的拷贝
	if (server_instance == NULL)
	{
		if (req->info_type == MICA_GET_RESPONSE)
			*is_async = true;	// 这里为true的含义是不让 rw 处理线程释放掉 recv_mr 区域
		else
			*is_async = false;
		__dhmp_send_respone_handler(rdma_trans, msg, PARTITION_NUMS);
		return;
	}

	switch (req->info_type)
	{
		case MICA_GET_RESPONSE:
		case MICA_SET_RESPONSE:
			__dhmp_send_respone_handler(rdma_trans, msg, PARTITION_NUMS);
			*is_async = false;
			break;			
		default:
			ERROR_LOG("Unknown request info_type %d", req->info_type);
			Assert(0);
			break;
	}

	return ;
}

// 间歇性的执行本地get操作，暂时中断当前的写操作
static void 
check_get_op(struct dhmp_msg* msg, size_t partition_id)
{
	bool need_post_recv;
	int i, op_gap;
	// get_is_more 为真 为假 现在 都可能执行这个函数

	// 如果set操作多，那么可以由set操作触发get
	if (!(server_instance->server_id == 0 && !main_node_is_readable))
	{
		for (i=little_idx; i<end_round; i++)
		{
			INFO_LOG("msg->main_thread_set_id [%d]", msg->main_thread_set_id);
			op_gap = op_gaps[i];
			if (msg->main_thread_set_id % op_gap == 0)
			{
				int get_id = msg->main_thread_set_id / op_gap;
				Assert(get_id <= read_num);
				struct dhmp_msg* get_msg = get_msgs_group[get_id];
				// get 操作现在不区分 partition_id 
				__dhmp_wc_recv_handler(get_msg->trans, get_msg, partition_id, &need_post_recv);
				// 不需要 post_recv
				//ERROR_LOG("distribute [%d] get task, now main_thread_set_id is [%d], get_id is [%d]", 1, msg->main_thread_set_id, get_id);
			}
		}
	}
}