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
#include "dhmp.h"
#include "midd_mica_benchmark.h"

#define MAIN_LOG_DEBUG_THROUGHOUT
volatile bool replica_is_ready = true;

struct timespec start_through, end_through;
uint64_t set_counts = 0, get_counts=0, op_counts=0;
long long int total_set_time = 0, total_get_time =0;
long long int total_set_latency_time = 0, total_set_through_time=0;
long long int total_through_time = 0;

static struct dhmp_msg * make_basic_msg(struct dhmp_msg * res_msg, struct post_datagram *resp, enum dhmp_msg_type type);
static void  dhmp_send_respone_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, bool *is_async);
void dhmp_send_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, bool * is_async);

struct mica_work_context mica_work_context_mgr[2];
unsigned long long  __partition_nums;

// 吞吐计数
int avg_partition_count_num=0;
int partition_set_count[PARTITION_MAX_NUMS];
bool partition_count_set_done_flag[PARTITION_MAX_NUMS]; 

// 延迟计数
int partition_0_count_num = 0;
struct list_head partition_local_send_list[PARTITION_MAX_NUMS];   
struct list_head main_thread_send_list[PARTITION_MAX_NUMS];
uint64_t partition_work_nums[PARTITION_MAX_NUMS];

size_t SERVER_ID= (size_t)-1;

// 由于 send 操作可能会被阻塞住，所以必须将 recv 操作让另一个线程处理，否则会出现死锁。
// 我们对每一个 partition 启动两个线程
void* mica_work_thread(void *data);

int get_req_partition_id(struct post_datagram *req);
int get_resp_partition_id(struct post_datagram *req);
void distribute_partition_resp(int partition_id, struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);

static void __dhmp_send_respone_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, size_t partition_id);
static void __dhmp_send_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, size_t partition_id, bool * need_post_recv);
void dhmp_mica_set_request_handler(struct dhmp_transport* rdma_trans, struct post_datagram *req, int partition_id);

static void dump_get_status(MICA_GET_STATUS get_status);

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
	Assert(false);
	rdma_trans;
	req;
	return NULL;
}

static void 
dhmp_mica_get_cli_MR_response_handler(struct dhmp_transport* rdma_trans,
													struct dhmp_msg* msg)
{
	Assert(false);
	rdma_trans;
	msg;
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

	__sync_fetch_and_sub(&resp->req_ptr->reuse_done_count, (uint32_t)1);

	if(resp->node_id == 1)
		resp->repllica1  = 1;
	if(resp->node_id == 2)
		resp->repllica1  = 2;
	if(resp->node_id == 3)
		resp->repllica1  = 3;		
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

	INFO_LOG("Get tag [%d], key is [%ld]", req_info->tag, *(size_t*)key_addr);

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

	dump_get_status(get_status);
	// TODO 增加get失败的原因：不存在还是version不一致
	if (re== false)
	{
		set_result->out_expire_time = 0;
		set_result->out_value_length = (size_t)-1;
		//ERROR_LOG("Node [%d] GET tag [%d] failed!", server_instance->server_id, req_info->tag);
	}
	else
	{
		set_result->out_expire_time = out_expire_time;
		set_result->out_value_length = in_out_value_length;
		INFO_LOG("Node [%d] GET tag [%d], tag is [%ld] success!, get value is %u", server_instance->server_id, req_info->tag, req_info->tag, *(size_t*) &(set_result->out_value[0]));
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

	WARN_LOG("[dhmp_mica_update_notify_request_handler] get tag [%ld]", req_info->tag);

	key_align_length = MEHCACHED_ROUNDUP8(MEHCACHED_KEY_LENGTH(update_item->kv_length_vec));
	value_len = MEHCACHED_VALUE_LENGTH(update_item->kv_length_vec);
	true_value_len = value_len - VALUE_HEADER_LEN - VALUE_TAIL_LEN;

	item_offset = get_offset_by_item(table, update_item);
	Assert(item_offset == req_info->item_offset);

    value_base = (struct midd_value_header*)(update_item->data + key_align_length);
    // value_data = update_item->data + key_align_length + VALUE_HEADER_LEN;
    // value_tail = (struct midd_value_tail*) VALUE_TAIL_ADDR(update_item->data , key_align_length, true_value_len);

	WARN_LOG("Node [%d] recv update value, now value version is [%ld]", \
				server_instance->server_id, value_base->version);

	// 在收到上游节点传递来的value后，继续向下游节点传播
	if (!IS_TAIL(server_instance->server_type))
	{
		// 生成 nic 任务下放到网卡发送链表
        makeup_update_request(update_item, item_offset, (uint8_t *)value_base, value_len, req_info->tag);
		INFO_LOG("Node [%d] continue send value to downstream node", server_instance->server_id);
	}

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

	// if (server_instance == NULL)
	// 	dev = dhmp_get_dev_from_client();
	// else
	// 	dev = dhmp_get_dev_from_server();

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
		case DHMP_MICA_SEND_2PC_REQUEST:
		case MICA_SET_REQUEST:
			INFO_LOG ( "Recv [MICA_SET_REQUEST] from node [%d]",  req->node_id);
			dhmp_mica_set_request_handler(rdma_trans, req, partition_id);
			if (server_instance->server_id == 0)
				*need_post_recv = false;	// 主节点 本地写不需要 post recv
			return; 	// 直接返回
		case MICA_GET_REQUEST:
			INFO_LOG ( "Recv [MICA_GET_REQUEST] from node [%d]",  req->node_id);
			resp = dhmp_mica_get_request_handler(req);
			*need_post_recv = false;	// 本地读不需要 post recv
			return;  // 直接返回
		case MICA_REPLICA_UPDATE_REQUEST:
			INFO_LOG ( "Recv [MICA_REPLICA_UPDATE_REQUEST] from node [%d]",  req->node_id);
			// 我们不需要再向上游 nic 发送确认，直接启动向下游节点 nic 的数据传输
			// resp = dhmp_mica_update_notify_request_handler(req);
			dhmp_mica_update_notify_request_handler(req);
			return;
		default:
			ERROR_LOG("Unknown request info_type %d", req->info_type);
			exit(0);
			break;
	}

	res_msg.msg_type = DHMP_MICA_SEND_INFO_RESPONSE;
	res_msg.data_size = DATAGRAM_ALL_LEN(resp->info_length);
	res_msg.data= resp;
	INFO_LOG("Send response msg length is [%u] KB", res_msg.data_size / 1024);

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
			//INFO_LOG ( "Recv [MICA_ACK_RESPONSE] from node [%d]",  resp->node_id);
			dhmp_ack_response_handler(rdma_trans, msg);
			break;	
		case MICA_GET_CLIMR_RESPONSE:
			Assert(false);
			break;
		case MICA_SERVER_GET_CLINET_NODE_ID_RESPONSE:
			//INFO_LOG ( "Recv [MICA_SERVER_GET_CLINET_NODE_ID_RESPONSE] from node [%d]",  resp->node_id);
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

// 原始的 wc_recv 函数，不会执行线程分发操作
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
make_basic_msg(struct dhmp_msg * res_msg, struct post_datagram *resp, enum dhmp_msg_type type )
{
	res_msg->msg_type = type;
	res_msg->data_size = DATAGRAM_ALL_LEN(resp->info_length);
	res_msg->data= resp;
}

int init_mulit_server_work_thread()
{
	bool recv_mulit_threads_enable=true;
	int i, retval;
	cpu_set_t cpuset;
	memset(&(mica_work_context_mgr[0]), 0, sizeof(struct mica_work_context));
	memset(&(mica_work_context_mgr[1]), 0, sizeof(struct mica_work_context));
	memset(partition_count_set_done_flag, 0, sizeof(bool) * PARTITION_MAX_NUMS);

	for (i=0; i<PARTITION_NUMS; i++)
	{
		CPU_ZERO(&cpuset);
		if (SERVER_ID < 4)
			CPU_SET(i, &cpuset);
		else
			CPU_SET(i+20, &cpuset);
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

		retval = pthread_getaffinity_np(mica_work_context_mgr[DHMP_MICA_SEND_INFO_REQUEST].threads[i], sizeof(cpu_set_t), &cpuset);
		if (retval)
		{
			printf("get cpu affinity failed");
			return -1;
		}
	}
}

int  get_req_partition_id(struct post_datagram *req)
{
	switch (req->info_type)
	{
		case DHMP_MICA_SEND_2PC_REQUEST:
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
	int retry_count=0;

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
		bool need_post_recv = true;
		__dhmp_wc_recv_handler(rdma_trans, msg, PARTITION_NUMS, &need_post_recv);

		// 间歇性的执行 get操作，暂时中断当前的写操作
		if (!is_all_set_all_get && msg->main_thread_set_id != -1)
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
			//if (partition_work_nums[partition_id] <= 50 ) // || retry_count >= 500
			if (partition_work_nums[partition_id] <= 50 || retry_count >= 500)
				break;
			else
				retry_count++;
		}

		retry_count=0;
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
	int retry_count = 0;

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
		// if (partition_work_nums[partition_id] >=50 || retry_count>=1000)
		if (partition_work_nums[partition_id] >=0 || retry_count>=1000)
		{
			retry_count=0;
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
					// 发送双边操作的数据大小不能超过  SINGLE_NORM_RECV_REGION （16MB）
					dhmp_post_recv(msg->trans, msg->data - sizeof(enum dhmp_msg_type) - sizeof(size_t), msg->recv_partition_id);
					free(container_of(&(msg->data), struct dhmp_msg , data));	// #define container_of(ptr, type, member)
				}

				// 间歇性的执行 get操作，暂时中断当前的写操作
				if (!is_all_set_all_get && msg->main_thread_set_id != -1)
					check_get_op(msg, partition_id);
			}
			// 将本地头节点重置为空
			INIT_LIST_HEAD(&partition_local_send_list[partition_id]);
		}
		else
			retry_count++;
	}
}

static void dump_get_status(MICA_GET_STATUS get_status)
{
	switch (get_status)
    {
        case MICA_GET_SUCESS:
			INFO_LOG("MICA_GET_SUCESS");
            break;
        case MICA_NO_KEY:
			INFO_LOG("MICA_NO_KEY");
            break;
        case MICA_VERSION_IS_DIRTY:
			INFO_LOG("MICA_VERSION_IS_DIRTY");
			break;
        case MICA_GET_PARTIAL:
			INFO_LOG("MICA_GET_PARTIAL");
            break;
        default:
			INFO_LOG("Unknow get status!");
            break;
    }
}

#define MAIN_LOG_DEBUG
void
dhmp_mica_set_request_handler(struct dhmp_transport* rdma_trans, struct post_datagram *req, int partition_id)
{
	struct timespec start_g, end_g;
	struct post_datagram *resp;
	struct dhmp_mica_set_request  * req_info;
	struct dhmp_mica_set_response * set_result;
	size_t resp_len = sizeof(struct dhmp_mica_set_response);
	struct mehcached_item * item;
	struct mehcached_table *table = &table_o;
	bool is_update, is_maintable = true;
	struct dhmp_msg resp_msg, req_msg;
	// struct dhmp_msg *resp_msg_ptr, *req_msg_ptr;

	void * key_addr;
	void * value_addr;

	// 时间戳
	struct timespec time_set = {0, 0};
	DEFINE_STACK_TIMER();

	if (IS_REPLICA(server_instance->server_type))
		Assert(replica_is_ready == true);

	req_info  = (struct dhmp_mica_set_request *) DATA_ADDR(req, 0);
	key_addr = (void*)req_info->data;	

	if (IS_MAIN(server_instance->server_type) || req_info->onePC)
		value_addr = (void*)key_addr + req_info->key_length;

#ifdef MAIN_LOG_DEBUG_LATENCE
	if (req_info->partition_id == 0)
		clock_gettime(CLOCK_MONOTONIC, &start_g);	
#endif

	resp = (struct post_datagram *) malloc(DATAGRAM_ALL_LEN(resp_len));
	memset(resp, 0, DATAGRAM_ALL_LEN(resp_len));
	set_result = (struct dhmp_mica_set_response *) DATA_ADDR(resp, 0);

	// INFO_LOG("Get set tag[%ld]", req_info->tag);

	// 注意！，此处不需要调用 warpper 函数
	// 因为传递过来的 value 地址已经添加好头部和尾部了，长度也已经加上了头部和尾部的长度。
	if (/*IS_MAIN(server_instance->server_type) || */ req_info->onePC)
	{
		// MICA_TIME_COUNTER_INIT();
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
		// MICA_TIME_COUNTER_CAL("[dhmp_mica_set_request_handler]->[mehcached_set]")
		
		// 大概上，用set函数返回的时间作为各个 set 操作的时间顺序
		//clock_gettime(CLOCK_MONOTONIC, &time_set);   
		
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
			//INFO_LOG("MICA node [%d] get set request, set tag is \"%d\",  mapping id is [%u] , value addr is [%p]", \
							server_instance->server_id, req_info->tag, set_result->out_mapping_id, set_result->value_addr);
		}
		else
		{
			ERROR_LOG("MICA node [%d] get set request, set tag is \"%d\", set key FAIL!", \
					server_instance->server_id, req_info->tag);
			set_result->out_mapping_id = (size_t) - 1;
			set_result->is_success = false;
			Assert(false);
		}
	}
	else if (!IS_MAIN(server_instance->server_type) && !req_info->onePC)
	{
		set_result->partition_id = req_info->partition_id;
		set_result->out_mapping_id = (size_t) -1;
		set_result->is_success = true;
	}

	if (IS_MIRROR(server_instance->server_type))
	{
		// 填充 response 报文
		resp->req_ptr  = req->req_ptr;		    		// 原样返回对方用于消息完成时的报文的判别
		resp->resp_ptr = resp;							// 自身用于消息完成时报文的判别
		resp->node_id  = server_instance->server_id;	// 向对端发送自己的 node_id 用于身份辨识
		resp->info_type = MICA_SET_RESPONSE;
		resp->info_length = resp_len;
		resp->done_flag = false;						// request_handler 不关心 done_flag（不需要阻塞）
		
		// 回复主节点
		resp_msg.msg_type = DHMP_MICA_SEND_INFO_RESPONSE;
		resp_msg.data_size = DATAGRAM_ALL_LEN(resp->info_length);
		resp_msg.data= resp;
		dhmp_post_send(rdma_trans, &resp_msg, req_info->partition_id);
		free(resp);
		INFO_LOG("Mirror node send response to main, tag [%d], is onePC [%d]", req_info->tag, req_info->onePC);
	}

	if (IS_MAIN(server_instance->server_type))
	{
		int nid;
		req_info->onePC = true;
		req->reuse_done_count = server_instance->config.nets_cnt - 1;
		req->req_ptr = req;
		req->node_id = MAIN;

		req_msg.msg_type = DHMP_MICA_SEND_INFO_REQUEST;
		req_msg.data_size = sizeof(struct post_datagram) + sizeof(struct dhmp_mica_set_request)\
							+ req_info->key_length + req_info->value_length;
		// req_msg.data_size = req->info_length;
		// req_msg.data_size = DATAGRAM_ALL_LEN(req->info_length);
		req_msg.data= req;

		// make_basic_msg(&req_msg, req, DHMP_MICA_SEND_INFO_REQUEST);
		// 1PC : 主节点等待各个副本节点的相应情况
		for (nid=MIRROR_NODE_ID ; nid< server_instance->config.nets_cnt; nid++)
			dhmp_post_send(find_connect_server_by_nodeID(nid), &req_msg, req_info->partition_id);
		
		// INFO_LOG("Main node send onePC tag[%ld]", req_info->tag);

		//MICA_TIME_COUNTER_INIT();
		while(req->reuse_done_count != 0);
			//MICA_TIME_LIMITED(req_info->tag, TIMEOUT_LIMIT_MS);

		INFO_LOG("Main node recv all 1PC, tag [%d]", req_info->tag);
		//clock_gettime(CLOCK_MONOTONIC, &end_g);	

		// 我们全部使用本地读写，不使用客户端
		// make_basic_msg(&resp_msg, resp, DHMP_MICA_SEND_INFO_RESPONSE);
		// dhmp_post_send(rdma_trans, &resp_msg, req_info->partition_id);	
		// INFO_LOG("Main node send response to client, tag [%d]", req_info->tag);

		// 2PC : 不发送 value
		req_info->onePC = false;
		req->info_length = sizeof(struct post_datagram) + sizeof(struct dhmp_mica_set_request) + req_info->key_length;
		req->reuse_done_count = server_instance->config.nets_cnt - 1;
		req->info_type = DHMP_MICA_SEND_2PC_REQUEST;
		// make_basic_msg(&req_msg, req, DHMP_MICA_SEND_INFO_REQUEST);
		req_msg.data_size = DATAGRAM_ALL_LEN(resp->info_length);

#ifdef DHMP_POST_SEND_LATENCE
		if (req_info->partition_id == 0)
			clock_gettime(CLOCK_MONOTONIC, &start_g);	
#endif
		// for (nid=MIRROR_NODE_ID ; nid< server_instance->config.nets_cnt; nid++)
		// 	dhmp_post_send(find_connect_server_by_nodeID(nid), &req_msg, req_info->partition_id);
#ifdef DHMP_POST_SEND_LATENCE
		if (server_instance->server_id == 0  &&
			req_info->partition_id == 0)
		{
			clock_gettime(CLOCK_MONOTONIC, &end_g);
			partition_0_count_num++;
			total_set_latency_time += ((((end_g.tv_sec * 1000000000) + end_g.tv_nsec) - ((start_g.tv_sec * 1000000000) + start_g.tv_nsec)));
			if (partition_0_count_num == avg_partition_count_num)
				ERROR_LOG("[%d] dhmp_post_send count avg time is [%lld] ns", partition_0_count_num, total_set_latency_time / (partition_0_count_num));
		}
#endif
		// INFO_LOG("Main node send twoPC tag[%ld]", req_info->tag);

		//MICA_TIME_COUNTER_INIT();
		// 优化，等下次合并传输
		// while(req->reuse_done_count != 0 );
			//MICA_TIME_LIMITED(req_info->tag, TIMEOUT_LIMIT_MS);

		// INFO_LOG("Main node recv all 2PC, tag [%d]", req_info->tag);

		partition_set_count[req_info->partition_id]++;
		if (partition_set_count[req_info->partition_id] == avg_partition_count_num)
			partition_count_set_done_flag[req_info->partition_id] = true;
#ifdef MAIN_LOG_DEBUG_LATENCE
		if (server_instance->server_id == 0  &&
			req_info->partition_id == 0)
		{
			if (partition_0_count_num==0)
				partition_0_count_num++;
			else
			{
				long long int latency;
				clock_gettime(CLOCK_MONOTONIC, &end_g);
				latency= ((((end_g.tv_sec * 1000000000) + end_g.tv_nsec) - ((start_g.tv_sec * 1000000000) + start_g.tv_nsec)));
				fprintf(stderr, "%ld ", latency/1000);
				total_set_latency_time += latency;
				partition_0_count_num++;
				if (partition_0_count_num == avg_partition_count_num)
				{
					// sleep(1);
					//ERROR_LOG("\n");
					//ERROR_LOG("[%d] set count avg time is [%lld]us", partition_0_count_num, total_set_latency_time / (US_BASE*(partition_0_count_num)));
				}
			}
		}
#endif
	}
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
		case MICA_ACK_RESPONSE:
		case MICA_GET_CLIMR_RESPONSE:
		case MICA_SERVER_GET_CLINET_NODE_ID_RESPONSE:
		case MICA_REPLICA_UPDATE_RESPONSE:
		case MICA_GET_RESPONSE:
		case MICA_SET_RESPONSE:
			// 非分区的操作就在主线程执行（可能的性能问题，也许需要一个专门的线程负责处理非分区的操作，但是非分区操作一般是初始化操作，对后续性能影响不明显）
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

// 我们只让 server 端按照分区进行多线程处理，客户端不会多线程化。 
void dhmp_send_request_handler(struct dhmp_transport* rdma_trans,
									struct dhmp_msg* msg, 
									bool * is_async)
{
	int i;
	struct post_datagram *req = (struct post_datagram*)(msg->data);
	struct timespec end;
	bool is_get=true;
	bool is_need_post_recv=true;

	switch (req->info_type)
	{
		case MICA_ACK_REQUEST:
		case MICA_SERVER_GET_CLINET_NODE_ID_REQUEST:
			// 非分区的操作就在主线程执行（可能的性能问题，也许需要一个专门的线程负责处理非分区的操作，但是非分区操作一般是初始化操作，对后续性能影响不明显）
			__dhmp_send_request_handler(rdma_trans, msg, PARTITION_NUMS, &is_need_post_recv);
			*is_async = false;
			Assert(is_need_post_recv == true);
			break;
		case MICA_SET_REQUEST:
			set_counts++;
			is_get=false;
		case DHMP_MICA_SEND_2PC_REQUEST:
		case MICA_GET_REQUEST:
#ifdef THROUGH_TEST
			if (op_counts ==0)
				clock_gettime(CLOCK_MONOTONIC, &start_through);  
#endif				
			// 分区的操作需要分布到特定的线程去执行
			//clock_gettime(CLOCK_MONOTONIC, &end);
			//INFO_LOG("Distribute [%ld] ns", (((end.tv_sec * 1000000000) + end.tv_nsec) - ((time_start1 * 1000000000) + time_start2)));
			if (!is_get)
				msg->main_thread_set_id = set_counts; 	// 必须在主线程设置全局唯一的 set_id

			// 不要忘记执行作为触发者的set
			distribute_partition_resp(get_req_partition_id(req), rdma_trans, msg);

			// 分区的操作需要分布到特定的线程去执行
			if (!is_get)
			{
				if (get_is_more && little_idx != 0)
				{
					// Assert(get_is_more);
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

#ifdef THROUGH_TEST
			op_counts++;
			if (server_instance->server_id == 0 && op_counts == update_num)
			{
				bool done_flag;
				while (true)
				{
					done_flag = true;
					for (i=0; i<PARTITION_NUMS; i++)
						done_flag &= partition_count_set_done_flag[i];
					
					if (done_flag)
						break;
				}
#ifndef PERF_TEST
				clock_gettime(CLOCK_MONOTONIC, &end_through); 
				total_through_time = ((((end_through.tv_sec * 1000000000) + end_through.tv_nsec) - ((start_through.tv_sec * 1000000000) + start_through.tv_nsec)));
				ERROR_LOG("set op count [%d], total op count [%d] total time is [%d] us", op_counts, __access_num, total_through_time / 1000);

				size_t total_ops_num=0;
				for (i=0; i<(int)PARTITION_NUMS; i++)
				{
					ERROR_LOG("partition[%d] set count [%d]",i, partition_set_count[i]);
					total_ops_num+=partition_set_count[i];
				}
				ERROR_LOG("Local total_ops_num is [%d], read_count is [%d]", total_ops_num,total_ops_num-update_num );
#endif
			}
#endif

			*is_async = true;
			break;
		default:
			ERROR_LOG("Unknown request info_type %d", req->info_type);
			exit(0);
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
			//INFO_LOG("msg->main_thread_set_id [%d]", msg->main_thread_set_id);
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