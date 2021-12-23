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
	INFO_LOG("resp_all_mapping_ptr is [%p], [%p]", resp_mapping_ptr, req_data->info_revoke_ptr);

	if (!dhmp_post_send_info(target_id, base, total_length, NULL))
	{
		ERROR_LOG("POST_SEND ERROR! target_id is [%d], info_length is [%u]", target_id, total_length);
		return;
	}

	while(req_msg->done_flag == false);
out:
	free(base);
	return;
}

enum ack_info_state
mica_basic_ack_req(size_t target_id, enum ack_info_type ack_type)
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

	while(req_msg->done_flag == false);

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

	while(req_msg->done_flag == false);

	result = req_data->node_id;
	free(base);
	return result;
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