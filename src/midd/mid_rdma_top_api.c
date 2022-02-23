#include "dhmp.h"
#include "dhmp_log.h"
#include "dhmp_config.h"
#include "dhmp_context.h"
#include "dhmp_dev.h"
#include "dhmp_transport.h"
#include "dhmp_task.h"
#include "dhmp_client.h"
#include "dhmp_server.h"
#include "dhmp_log.h"
#include "mid_rdma_utils.h"
#include "dhmp_top_api.h"

static bool 
dhmp_post_send_info(size_t target_id, void * data, size_t length, struct dhmp_transport *specify_trans);

struct dhmp_transport* 
dhmp_get_trans_from_addr(void *dhmp_addr)
{
	long long node_index=(long long)dhmp_addr;
	node_index=node_index>>48;
	return client_mgr->connect_trans[node_index];
}

void
micaserver_get_cliMR(struct replica_mappings  *resp_mapping_ptr, size_t target_id)
{
	void * base;
	struct post_datagram *req_msg;
	struct dhmp_mica_get_cli_MR_request *req_data;
	size_t total_length = 0;
	bool re;

	// size_t target_id = server_instance->server_id + 1;
	// Assert(target_id != server_instance->node_nums);

	// 构造报文
	total_length = DATAGRAM_ALL_LEN(sizeof(struct dhmp_mica_get_cli_MR_request));
	base = malloc(total_length); 
	req_msg  = (struct post_datagram *) base;
	req_data = (struct dhmp_mica_get_cli_MR_request *) DATA_ADDR(base, 0);

	// 填充公共报文
	req_msg->node_id = server_instance->server_id;	 // 向对端发送自己的 node_id 用于身份辨识
	req_msg->req_ptr = req_msg;
	req_msg->resp_ptr = NULL;
	req_msg->done_flag = false;
	req_msg->info_type = MICA_GET_CLIMR_REQUEST;
	req_msg->info_length = sizeof(struct dhmp_mica_get_cli_MR_request);

	// 填充私有报文
	req_data->info_revoke_ptr = resp_mapping_ptr;
	INFO_LOG("resp_all_mapping_ptr is [%p]", resp_mapping_ptr);

	if (!dhmp_post_send_info(target_id, base, total_length, NULL))
	{
		ERROR_LOG("POST_SEND ERROR! target_id is [%d], info_length is [%u]", target_id, total_length);
		return;
	}

	MICA_TIME_COUNTER_INIT();
	while(req_msg->done_flag == false)
		MICA_TIME_COUNTER_CAL();

out:
	free(base);
	return;
}

// 如果 block 为 false 则不等待对端发送ACK，返回值没有意义
enum ack_info_state
mica_basic_ack_req(size_t target_id, enum ack_info_type ack_type, bool block)
{
	void * base;
	struct post_datagram *req_msg;
	struct dhmp_mica_ack_request *req_data;
	size_t total_length = 0;
	bool re;
	enum ack_info_state resp_state;

	// 构造报文
	total_length = sizeof(struct post_datagram) + sizeof(struct dhmp_mica_ack_request);
	base = malloc(total_length); 
	req_msg  = (struct post_datagram *) base;
	req_data = (struct dhmp_mica_ack_request *)((char *)base + sizeof(struct post_datagram));

	// 填充公共报文
	req_msg->node_id = server_instance->server_id;	 // 向对端发送自己的 node_id 用于身份辨识
	req_msg->req_ptr = req_msg;
	req_msg->done_flag = false;
	req_msg->info_type = MICA_ACK_REQUEST;
	req_msg->info_length = sizeof(struct dhmp_mica_ack_request);

	// 填充私有报文
	req_data->ack_type = ack_type;

	if (!dhmp_post_send_info(target_id, base, total_length, NULL))
	{
		ERROR_LOG("POST_SEND ERROR! target_id is [%d], info_length is [%u]", target_id, total_length);
		return -1;
	}

	if (block)
	{
		MICA_TIME_COUNTER_INIT();
		while(req_msg->done_flag == false)
			MICA_TIME_COUNTER_CAL();
	}

	// 我们直接将 response 报文中的 ack_info_state 复制到request 报文中的 
	// ack_info_type 字段，因为二者都为枚举类型，所以是赋值安全的
	resp_state = req_data->ack_type;
	free(base);
	return resp_state;
}

int
mica_ask_nodeID_req(struct dhmp_transport* new_rdma_trans)
{
	void * base;
	struct post_datagram *req_msg;
	struct dhmp_get_nodeID_request *req_data;
	size_t total_length = 0;
	int result = -1;

	// 构造报文
	total_length = sizeof(struct post_datagram) + sizeof(struct dhmp_get_nodeID_request);
	base = malloc(total_length); 
	req_msg  = (struct post_datagram *) base;
	req_data = (struct dhmp_get_nodeID_request *)((char *)base + sizeof(struct post_datagram));

	// 填充公共报文
	req_msg->node_id = server_instance->server_id;	 // 向对端发送自己的 node_id 用于身份辨识
	req_msg->req_ptr = req_msg;
	req_msg->done_flag = false;
	req_msg->info_type = MICA_SERVER_GET_CLINET_NODE_ID_REQUEST;
	req_msg->info_length = sizeof(struct dhmp_get_nodeID_request);

	// 填充私有报文
	req_data->node_id = server_instance->server_id;

	if (!dhmp_post_send_info(-1, base, total_length, new_rdma_trans))
		return -1;

	MICA_TIME_COUNTER_INIT();
	while(req_msg->done_flag == false)
		MICA_TIME_COUNTER_CAL();

	result = req_data->node_id;
	free(base);
	return result;
}

/*
uint8_t current_alloc_id, struct mehcached_table *table, uint64_t key_hash,\
                const uint8_t *key, size_t key_length, const uint8_t *value, size_t value_length,\
                uint32_t expire_time, bool overwrite
// 注意 mica_set_remote 函数删除了 table 参数
*/
static size_t
mica_set_remote(uint8_t current_alloc_id,  uint64_t key_hash, const uint8_t *key, 
				size_t key_length, const uint8_t *value, size_t value_length,
                uint32_t expire_time, bool overwrite, 
				bool is_async, 
				struct set_requset_pack * req_callback_ptr,
				size_t target_id,
				bool is_update)
{
	void * base;
	void * data_addr;
	struct post_datagram *req_msg;
	struct dhmp_mica_set_request *req_data;
	size_t total_length = 0;
	size_t re_mapping_id;

	// HexDump((char*)key, (int) (key_length + value_length), (size_t)key);

	// 构造报文
	// if (target_id == MIRROR_NODE_ID)
		total_length = sizeof(struct post_datagram) + sizeof(struct dhmp_mica_set_request) + key_length + value_length;
	// else
		// total_length = sizeof(struct post_datagram) + sizeof(struct dhmp_mica_set_request) + key_length;
	
	base = malloc(total_length); 
	// memset(base, 0 , total_length);
	req_msg  = (struct post_datagram *) base;
	req_data = (struct dhmp_mica_set_request *)((char *)base + sizeof(struct post_datagram));

	// 填充公共报文
	req_msg->node_id = server_instance->server_id;	 // 向对端发送自己的 node_id 用于身份辨识
	req_msg->req_ptr = req_msg;
	req_msg->done_flag = false;
	req_msg->info_type = MICA_SET_REQUEST;
	req_msg->info_length = sizeof(struct dhmp_mica_set_request);

	// 填充私有报文
	req_data->current_alloc_id = current_alloc_id;
	req_data->expire_time = expire_time;
	req_data->key_hash = key_hash;
	req_data->key_length = key_length;
	req_data->value_length = value_length;	// 这里的 value 长度是包含了value头部和尾部的长度
	req_data->overwrite = overwrite;
	req_data->is_update = is_update;
	data_addr = (void*)req_data + offsetof(struct dhmp_mica_set_request, data);
	memcpy(data_addr, key, key_length);		// copy key

	// if (target_id == MIRROR_NODE_ID)
	memcpy(data_addr + key_length, value, value_length);	// copy value, 注意这里拷贝不包含value的头部和尾部，所以需要远端节点自己进行元数据的更新

	if (!dhmp_post_send_info(target_id, base, total_length, NULL))
		return false;

	if (is_async == false)
	{
		MICA_TIME_COUNTER_INIT();
		while(req_msg->done_flag == false)
			MICA_TIME_COUNTER_CAL();

		if (req_data->is_success == false)
		{
			ERROR_LOG("remote set node [%d] is falied!", req_msg->node_id );
			Assert(false);
		}

		re_mapping_id = req_data->out_mapping_id;
		free(base);
	}
	else
	{
		req_callback_ptr->req_ptr = req_msg;
		req_callback_ptr->req_info_ptr = req_data;
	}

	return re_mapping_id;
}

static struct dhmp_mica_get_response*
mica_get_remote(uint8_t current_alloc_id,  uint64_t key_hash, const uint8_t *key, 
				size_t key_length, 
				bool is_async, 
				struct set_requset_pack * req_callback_ptr,
				size_t target_id)
{
	void * base;
	void * data_addr;
	struct post_datagram *req_msg;
	struct dhmp_mica_get_request *req_data;
	size_t total_length = 0;
	struct dhmp_mica_get_response* get_resp = (struct dhmp_mica_get_response*)\
					 malloc(sizeof(struct dhmp_mica_get_response) + MICA_DEFAULT_VALUE_LEN);

	// 构造报文
	total_length = sizeof(struct post_datagram) + sizeof(struct dhmp_mica_get_request) + key_length;
	base = malloc(total_length); 
	req_msg  = (struct post_datagram *) base;
	req_data = (struct dhmp_mica_get_request *)((char *)base + sizeof(struct post_datagram));

	// 填充公共报文
	req_msg->node_id = server_instance->server_id;	 // 向对端发送自己的 node_id 用于身份辨识
	req_msg->req_ptr = req_msg;
	req_msg->done_flag = false;
	req_msg->info_type = MICA_GET_REQUEST;
	req_msg->info_length = sizeof(struct dhmp_mica_get_request);

	// 填充私有报文
	req_data->current_alloc_id = current_alloc_id;
	req_data->key_hash = key_hash;
	req_data->key_length = key_length;
	req_data->get_resp = get_resp;
	data_addr = (void*)req_data + offsetof(struct dhmp_mica_get_request, data);
	memcpy(data_addr, key, key_length);		// copy key

	if (!dhmp_post_send_info(target_id, base, total_length, NULL))
		return NULL;

	if (is_async == false)
	{
		MICA_TIME_COUNTER_INIT();
		while(req_msg->done_flag == false)
			MICA_TIME_COUNTER_CAL();

		free(base);
		return get_resp;
	}
	else
	{
		assert(req_callback_ptr!=NULL);
		// 目前不支持异步的get
		assert(false);
	}

	return NULL;
}

void 
mica_replica_update_notify(uint64_t item_offset)
{
	// Assert(!IS_TAIL(server_instance->server_type));
	// INFO_LOG("mica_replica_update_notify offset is [%ld]", item_offset);
	// void * base;
	// void * data_addr;
	// struct post_datagram *req_msg;
	// struct dhmp_update_notify_request *req_data;
	// size_t total_length = 0;
	// size_t target_id;

	// if (IS_MAIN(server_instance->server_type))
	// 	target_id = REPLICA_NODE_HEAD_ID;
	// else if (IS_MIRROR(server_instance->server_type))
	// 	target_id = server_instance->server_id + 1;
	// else
	// 	Assert(false);

	// // 构造报文
	// total_length = sizeof(struct post_datagram) + sizeof(struct dhmp_update_notify_request);
	// base = malloc(total_length); 
	// req_msg  = (struct post_datagram *) base;
	// req_data = (struct dhmp_update_notify_request *)((char *)base + sizeof(struct post_datagram));

	// // 填充公共报文
	// req_msg->node_id = server_instance->server_id;	 // 向对端发送自己的 node_id 用于身份辨识
	// req_msg->req_ptr = req_msg;
	// req_msg->done_flag = false;
	// req_msg->info_type = MICA_REPLICA_UPDATE_REQUEST;
	// req_msg->info_length = sizeof(struct dhmp_update_notify_request);

	// // 填充私有报文
	// req_data->item_offset = item_offset;

	// if (!dhmp_post_send_info(target_id, base, total_length, NULL))
	// 	return;
	
	// MICA_TIME_COUNTER_INIT();
	// while(req_msg->done_flag == false)
	// 	MICA_TIME_COUNTER_CAL();

	// free(base);
}


bool 
dhmp_post_send_info(size_t target_id, void * data, size_t length, struct dhmp_transport *specify_trans)
{
	struct dhmp_transport *rdma_trans=NULL;
	struct dhmp_msg msg;

	if (target_id == -1)
	{
		rdma_trans = specify_trans;
		Assert(rdma_trans != NULL);
	}
	else
	{
		// 调用 client_send_info 前必须保证服务的已经初始化完成
		// 需要注意，谁主动发送请求，谁就是客户端，因此都是调用 dhmp_client_XX 的方法
		// 不管你是主节点还是从节点
		rdma_trans = find_connect_server_by_nodeID(target_id);

		if(!rdma_trans)
		{
			ERROR_LOG("don't exist remote server_instance. target_id=[%d]", target_id);
			return true;
		}
	}

	if(rdma_trans->trans_state!=DHMP_TRANSPORT_STATE_CONNECTED)
	{
		ERROR_LOG("ERROR! Transport is not CONNECTED!, now state is \"%s\"", dhmp_printf_connect_state(rdma_trans->trans_state));
		return false;
	}
	/*build malloc request msg*/
	msg.msg_type = DHMP_MICA_SEND_INFO_REQUEST;
	msg.data_size = length;
	msg.data= data;

	dhmp_post_send(rdma_trans, &msg);
out:
	return true;
}


size_t
mica_set_remote_warpper(uint8_t current_alloc_id,  
				const uint8_t* no_header_key, uint64_t key_hash,size_t true_key_length, 
				const uint8_t* no_header_value, size_t true_value_length,
                uint32_t expire_time, bool overwrite, 
				bool is_async, 
				struct set_requset_pack * req_callback_ptr,
				size_t target_id,
				bool is_update)
{
	return mica_set_remote(current_alloc_id, key_hash, 
				no_header_key, 
				true_key_length, 
				no_header_value, 
				true_value_length,
				expire_time, overwrite, is_async, req_callback_ptr, target_id,is_update);
}

struct dhmp_mica_get_response*
mica_get_remote_warpper(uint8_t current_alloc_id,  uint64_t key_hash, const uint8_t *key, 
				size_t key_length, 
				bool is_async, 
				struct set_requset_pack * req_callback_ptr,
				size_t target_id)
{
	return mica_get_remote(current_alloc_id,  key_hash, key, 
				key_length, 
				is_async, 
				req_callback_ptr,
				target_id);
}