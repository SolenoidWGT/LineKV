// "shm_private.h" 头文件要包含在 "dhmp_task.h" 等头文件之后
#include "shm_private.h"
#include "table.h"
#include "dhmp_client.h"
#include "dhmp_log.h"
#include "dhmp_dev.h"
#include "dhmp_server.h"
#include "mid_rdma_utils.h"
#include "dhmp_transport.h"
#include "dhmp_task.h"

static struct post_datagram * 
dhmp_mica_get_cli_MR_request_handler(struct dhmp_transport* rdma_trans,
									struct post_datagram *req)
{
	struct post_datagram *resp;
	struct dhmp_mica_get_cli_MR_request  * req_info, *resp_req;
	struct dhmp_mica_get_cli_MR_response * cli_mappings;
	struct dhmp_msg res_msg;
	struct dhmp_device * dev = dhmp_get_dev_from_server();
	size_t resp_len = 0;
	size_t req_len = res_msg.data_size;

	// 该节点的 mapping 信息和 mr 信息
	resp_len += sizeof(struct dhmp_mica_get_cli_MR_response);
	resp = (struct post_datagram *) malloc(DATAGRAM_ALL_LEN(resp_len));
	INFO_LOG("resp length is %u", DATAGRAM_ALL_LEN(resp_len));
	memset(resp, 0, DATAGRAM_ALL_LEN(resp_len));

	req_info  = (struct dhmp_mica_get_cli_MR_request *) DATA_ADDR(req, 0);
	resp_req  = (struct dhmp_mica_get_cli_MR_request *) DATA_ADDR(resp, 0);
	cli_mappings = (struct dhmp_mica_get_cli_MR_response *) DATA_ADDR(resp, sizeof(struct dhmp_mica_get_cli_MR_request));

	INFO_LOG("client revoke ptr is [%p]", req_info->info_revoke_ptr);
	INFO_LOG("resp addr is %p", resp);
	INFO_LOG("cli_mappings addr is %p", cli_mappings);
	INFO_LOG("offset is %u", (char*)cli_mappings - (char*)resp);
	INFO_LOG("resp_len is %u", resp_len);
	INFO_LOG("total len is %u", DATAGRAM_ALL_LEN(resp_len));
	INFO_LOG("sizeof(post_datagram) is %u", HEADER_LEN);
	INFO_LOG("sizeof(dhmp_mica_get_cli_MR_response) is %u", sizeof(struct dhmp_mica_get_cli_MR_response));
	
	// 保存客户端的回调指针
	memcpy(resp_req, req_info, sizeof(struct dhmp_mica_get_cli_MR_request));
	// 填充自身新产生的信息
	cli_mappings->resp_all_mapping.node_id = server_instance->server_id;
	cli_mappings->resp_all_mapping.used_mapping_nums = get_mapping_nums();
	cli_mappings->resp_all_mapping.first_inited = false;

	if (false == get_table_init_state())
	{
		mehcached_shm_lock();
		if (false == get_table_init_state() )
		{
			// 进行节点的 k-v 初始化
			struct mehcached_table *table = &table_o;
			size_t numa_nodes[] = {(size_t)-1};

			// 初始化 hash table 并注册内存
			mehcached_table_init(table, 1, 1, 256, false, false, false, \
								numa_nodes[0], numa_nodes, MEHCACHED_MTH_THRESHOLD_FIFO);
			Assert(table);

			cli_mappings->resp_all_mapping.first_inited = true;
			INFO_LOG("replica node[%d] first init finished!, caller is node[%d]", \
						server_instance->server_id, req->node_id);
			
			set_table_init_state(true);
		}
		mehcached_shm_unlock();
	}

	copy_mapping_info( (void*) cli_mappings->resp_all_mapping.mehcached_shm_pages);
	copy_mapping_mrs_info((struct ibv_mr *) (cli_mappings->resp_all_mapping.mrs));

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

	struct dhmp_mica_get_cli_MR_response *resp_info = \
			(struct dhmp_mica_get_cli_MR_response *) DATA_ADDR(resp, 0);

	memcpy(resp_info->request.info_revoke_ptr, \
		&resp_info->resp_all_mapping, sizeof(struct replica_mappings));

	INFO_LOG("Node [%d] get mr info from node[%d]!", server_instance->server_id, resp->node_id);
}

static struct post_datagram * 
dhmp_ack_request_handler(struct dhmp_transport* rdma_trans,
							struct post_datagram *req)
{
	// 我们不拷贝直接复用 request post_datagram
	struct post_datagram *resp;
	struct dhmp_mica_ack_response * resp_data = (struct dhmp_mica_ack_response *) DATA_ADDR(req, 0);
	enum ack_info_state resp_ack_state;

	switch (req->info_type)
	{
	case MICA_INIT_ADDR_ACK:
		mehcached_shm_lock();
		if (false == get_table_init_state())
			resp_ack_state = MICA_ACK_INIT_ADDR_NOT_OK;
		else
			resp_ack_state = MICA_ACK_INIT_ADDR_OK;
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

	return resp;
}

static void 
dhmp_ack_response_handler(struct dhmp_transport* rdma_trans,
													struct dhmp_msg* msg)
{
	struct post_datagram *resp = (struct post_datagram *) &(msg->data); 
	struct post_datagram *req  = (struct post_datagram *) resp->req_ptr;

	struct dhmp_mica_ack_request * req_info = \
			(struct dhmp_mica_ack_request *) DATA_ADDR(req, 0);
	struct dhmp_mica_ack_response *resp_info = \
			(struct dhmp_mica_ack_response *) DATA_ADDR(resp, 0);

	// 直接复用 dhmp_mica_ack_request 结构体放置返回值
	req_info->ack_type = resp_info->ack_state;
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

}

// 所有的双边 rdma 操作 request_handler 的路由入口
// 这个函数只暴露基本的数据报 post_datagram 结构体，不涉及具体的数据报内容
// 根据 info_type 去调用正确的回调函数对数据报进行处理。 
static void dhmp_send_request_handler(struct dhmp_transport* rdma_trans,
											struct dhmp_msg* msg)
{
	struct dhmp_msg res_msg;
	struct dhmp_device * dev = dhmp_get_dev_from_server();
	struct post_datagram *req;
	struct post_datagram *resp;
	req = (struct post_datagram*)msg->data;

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
		default:
			ERROR_LOG("Unknown request info_type %d", req->info_type);
			exit(0);
			break;
	}

	res_msg.msg_type = DHMP_MICA_SEND_INFO_RESPONSE;
	res_msg.data_size = DATAGRAM_ALL_LEN(resp->info_length);
	res_msg.data= resp;
	INFO_LOG("Send response msg length is [%u] KB", res_msg.data_size / 1024);
	dhmp_post_send(rdma_trans, &res_msg);

	// 需要注意的是，因为我们直接复用了 request ack 报文，所以 req->info_type 变成了
	// resp 的类型，所以下面的 if 判断是用 req 的指针判断了 response 的消息
	if (req->info_type != MICA_ACK_RESPONSE &&
	    req->info_type != MICA_SERVER_GET_CLINET_NODE_ID_RESPONSE)
		free((void*) resp);

	return ;
}

static void 
dhmp_send_respone_handler(struct dhmp_transport* rdma_trans,
													struct dhmp_msg* msg)
{
	struct post_datagram *resp;
	resp = (struct post_datagram*)msg->data;

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
		default:
			break;
	}

	resp->req_ptr->done_flag = true;
}


/**
 *	dhmp_wc_recv_handler:handle the IBV_WC_RECV event
 */
void dhmp_wc_recv_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	switch(msg->msg_type)
	{
		case DHMP_MICA_SEND_INFO_REQUEST:
			dhmp_send_request_handler(rdma_trans, msg);
			break;
		case DHMP_MICA_SEND_INFO_RESPONSE:
			dhmp_send_respone_handler(rdma_trans, msg);
			break;

		case DHMP_MSG_CLOSE_CONNECTION:
			rdma_disconnect(rdma_trans->cm_id);
			break;
		default:
			break;
	}
}
