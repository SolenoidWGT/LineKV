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
#include "table.h"
#include "util.h"
#include "alloc_dynamic.h"
#include "nic.h"

static struct dhmp_msg * make_basic_msg(struct dhmp_msg * res_msg, struct post_datagram *resp);

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

		if (IS_REPLICA() && !IS_TAIL() &&  nic_thread_ready == false)
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
dhmp_mica_set_request_handler(struct post_datagram *req)
{
	struct post_datagram *resp;
	struct dhmp_mica_set_request  * req_info;
	struct dhmp_mica_set_response * set_result;
	size_t resp_len = sizeof(struct dhmp_mica_set_response);
	struct mehcached_item * item;
	struct mehcached_table *table = &table_o;
	bool is_update, is_maintable = true;
	struct dhmp_msg resp_msg;

	void * key_addr;
	void * value_addr;

	if (IS_REPLICA())
		Assert(replica_is_ready == true);

	req_info  = (struct dhmp_mica_set_request *) DATA_ADDR(req, 0);
	key_addr = (void*)req_info->data;	

	Assert(!IS_MAIN());
	// 不管是 insert 还是 update 副本节点都需要等待上游网卡节点传送数据
	if (IS_REPLICA())
		value_addr = (void*) 0x1;
	else
		value_addr = (void*)key_addr + req_info->key_length;

	// 该节点的 mapping 信息和 mr 信息
	// 回传key（为了上游节点确定item，仅靠key_hash是不够的）
	if (!IS_HEAD() && !IS_MIRROR())
		resp_len += req_info->key_length;	

	resp = (struct post_datagram *) malloc(DATAGRAM_ALL_LEN(resp_len));
	memset(resp, 0, DATAGRAM_ALL_LEN(resp_len));
	set_result = (struct dhmp_mica_set_response *) DATA_ADDR(resp, 0);

	// 注意！，此处不需要调用 warpper 函数
	// 因为传递过来的 value 地址已经添加好头部和尾部了，长度也已经加上了头部和尾部的长度。
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

	// Assert(is_update == req_info->is_update);
	if (IS_REPLICA())
		Assert(is_maintable == true);

	if (item != NULL)
	{
		set_result->value_addr = (uintptr_t ) item_get_value_addr(item);
		set_result->out_mapping_id = item->mapping_id;
		set_result->is_success = true;
		INFO_LOG("MICA node [%d] get set request, set key_hash is \"%lx\",  mapping id is [%u] , value addr is [%p]", \
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

	// 拷贝 key 和 key 的长度
	if (!IS_HEAD() && !IS_MIRROR())
	{
		memcpy(set_result->key_data, key_addr, req_info->key_length);
		set_result->key_length = req_info->key_length;
		set_result->key_hash = req_info->key_hash;
	}

	// ONE_LOOP_TIMER 启动一个特殊的进程监视一次循环的时间
#ifndef ONE_LOOP_TIMER

#endif

	INFO_LOG("key_hash is %lx, len is %lu, addr is %p ", set_result->key_hash, set_result->key_length, key_addr);

	// 填充 response 报文
	resp->req_ptr  = req->req_ptr;		    		// 原样返回对方用于消息完成时的报文的判别
	resp->resp_ptr = resp;							// 自身用于消息完成时报文的判别
	resp->node_id  = server_instance->server_id;	// 向对端发送自己的 node_id 用于身份辨识
	resp->info_type = MICA_SET_RESPONSE;
	resp->info_length = resp_len;
	resp->done_flag = false;						// request_handler 不关心 done_flag（不需要阻塞）

	// 各副本节点（除了主节点直接负责的副本节点）还需要向各自的直接上游
	// 节点发送自己的新分配的 item 的 mappingID 和虚拟地址
	if (!IS_HEAD() && !IS_MIRROR())
	{
		// 发送给上游节点，我们是被动建立连接的一方，是服务端
		dhmp_post_send(find_connect_client_by_nodeID(server_instance->server_id - 1),\
						make_basic_msg(&resp_msg, resp));
	}

	return resp;
}

static void 
dhmp_set_response_handler(struct dhmp_msg* msg)
{
	struct post_datagram *resp = (struct post_datagram *) (msg->data); 
	struct dhmp_mica_set_response *resp_info = \
			(struct dhmp_mica_set_response *) DATA_ADDR(resp, 0);

	if (IS_REPLICA())
		Assert(replica_is_ready == true);

	if (IS_MAIN())
	{
		struct post_datagram *req = resp->req_ptr; 
		struct dhmp_mica_set_request *req_info = \
				(struct dhmp_mica_set_request *) DATA_ADDR(req, 0);

		req_info->out_mapping_id = resp_info->out_mapping_id;
		req_info->out_value_addr = resp_info->value_addr;
		req_info->is_success = resp_info->is_success;		// 新增的成功标识位，如果失败需要重试

		resp->req_ptr->done_flag = true;

		INFO_LOG("Node [%d] set key_hash [%lx] to node[%d]!, is success!", \
				server_instance->server_id, req_info->key_hash, resp->node_id);
	}
	else	// 上游节点（副本节点执行下面的逻辑）
	{ 
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
		INFO_LOG("key_hash is %lx, len is %lu, addr is %p ", key_hash, key_length, key_addr);
		while (true)
		{
			target_item = find_item(table, key_hash , key_addr, key_length);
			if (target_item != NULL)
				break;
		}

		target_item->mapping_id = resp_info->out_mapping_id;
		target_item->remote_value_addr = resp_info->value_addr;
		INFO_LOG("Node [%d] recv downstram node's key_hash \"[%lx]\"'s virtual addr info is %p", \
						server_instance->server_id, resp_info->key_hash, target_item->remote_value_addr);
	}
}

static struct post_datagram * 
dhmp_mica_get_request_handler(struct post_datagram *req)
{
	struct post_datagram *resp;
	struct dhmp_mica_get_request  * req_info;
	struct dhmp_mica_get_response * set_result;
	size_t resp_len;
	bool re;

	void * key_addr;
	void * value_addr;

	req_info  = (struct dhmp_mica_get_request *) DATA_ADDR(req, 0);
	key_addr = (void*)req_info->data;

	if (IS_REPLICA())
		Assert(replica_is_ready == true);

	// uint16_t partition_id = PARTITION_ID(req_info->key_hash);
	// 在调用 get 之前还不能确定需要返回的报文长度的大小
	// 但是处于简单和避免两次RPC交互，我们默认value的长度为1k
	resp_len = sizeof(struct dhmp_mica_get_response) + MICA_DEFAULT_VALUE_LEN;
	resp = (struct post_datagram *) malloc(DATAGRAM_ALL_LEN(resp_len));
	memset(resp, 0, DATAGRAM_ALL_LEN(resp_len));							// 考虑对齐，小心设计结构体的内存布局
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
						false);	// 我们获取带 header 和 tailer 的 value

	// TODO 增加get失败的原因：不存在还是version不一致
	if (re== false)
	{
		set_result->out_expire_time = 0;
		set_result->out_value_length = (size_t)-1;
		ERROR_LOG("Node [%d] GET key_hash [%lx] failed!", server_instance->server_id, req_info->key_hash);
	}
	else
	{
		if (in_out_value_length > MICA_DEFAULT_VALUE_LEN)
			set_result->partial = true;
		else
			set_result->partial = false;

		set_result->out_expire_time = out_expire_time;
		set_result->out_value_length = in_out_value_length;
		INFO_LOG("Node [%d] GET key_hash [%lx] success!, get value is %u", server_instance->server_id, req_info->key_hash,  *(size_t*) &(set_result->out_value[0]));
	}

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
dhmp_get_response_handler(struct dhmp_msg* msg)
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
	get_re_info->partial 			= resp_info->partial;

	if (resp_info->out_value_length != (size_t) -1)
	{
		void * value_addr =  (void*)&resp_info->out_value[0];
		memcpy(&get_re_info->out_value[0], value_addr, resp_info->out_value_length );
		INFO_LOG("Node [%d] GET key_hash [%lx] to node[%d]!, is success!", \
					server_instance->server_id, req_info->key_hash, resp->node_id);
	}
	else
	{
		ERROR_LOG("Node [%d] GET key_hash [%lx] to node[%d]!, is FAILED!", \
				server_instance->server_id, req_info->key_hash, resp->node_id);
	}

	resp->req_ptr->done_flag = true;
}

static struct post_datagram * 
dhmp_mica_update_notify_request_handler(struct post_datagram *req)
{
	Assert(IS_REPLICA());
	struct post_datagram *resp;
	struct dhmp_update_notify_request  * req_info;
	struct dhmp_update_notify_response * set_result;
	size_t resp_len = sizeof(struct dhmp_update_notify_response);

	// 访问 value 各部分的基址
	struct mehcached_item * update_item;
	struct mehcached_table *table = &table_o;
    struct midd_value_header* value_base;
    uint8_t* value_data;
    struct midd_value_tail* value_tail;
	size_t key_align_length;
	size_t value_len, true_value_len;
	uint64_t item_offset;

	if (IS_REPLICA())
		Assert(replica_is_ready == true);

	req_info  = (struct dhmp_update_notify_request *) DATA_ADDR(req, 0);
	update_item = get_item_by_offset(table, req_info->item_offset);

	key_align_length = MEHCACHED_ROUNDUP8(MEHCACHED_KEY_LENGTH(update_item->kv_length_vec));
	value_len = MEHCACHED_VALUE_LENGTH(update_item->kv_length_vec);
	true_value_len = value_len - VALUE_HEADER_LEN - VALUE_TAIL_LEN;

	item_offset = get_offset_by_item(table, update_item);
	Assert(item_offset == req_info->item_offset);

    value_base = (struct midd_value_header*)(update_item->data + key_align_length);
    value_data = update_item->data + key_align_length + VALUE_HEADER_LEN;
    value_tail = (struct midd_value_tail*) VALUE_TAIL_ADDR(update_item->data , key_align_length, true_value_len);

	INFO_LOG("Node [%d] recv update value, now value version is [%ld]", \
				server_instance->server_id, value_base->version);

	MICA_TIME_COUNTER_INIT();
	while (memcmp(&value_base->version, &value_tail->version, sizeof(uint64_t)) != 0)
		MICA_TIME_COUNTER_CAL();
	
	MICA_TIME_COUNTER_INIT();
	while (value_tail->dirty == true)
		MICA_TIME_COUNTER_CAL();

	INFO_LOG("Node [%d] finished update value, now value version is [%ld]", \
				server_instance->server_id, value_base->version);

	// 在收到上游节点传递来的value后，继续向下游节点传播
	if (!IS_TAIL())
	{
		// 生成 nic 任务下放到网卡发送链表
        makeup_update_request(update_item, item_offset, (uint8_t *)value_base, value_len);
		INFO_LOG("Node [%d] continue send value to downstream node", server_instance->server_id);
	}

	resp_len = sizeof(struct dhmp_update_notify_response);
	resp = (struct post_datagram *) malloc(DATAGRAM_ALL_LEN(resp_len));
	set_result = (struct dhmp_update_notify_response *) DATA_ADDR(resp, 0);
	set_result->is_success = true;

	// 填充 response 报文
	resp->req_ptr  = req->req_ptr;		    		// 原样返回对方用于消息完成时的报文的判别
	resp->resp_ptr = resp;							// 自身用于消息完成时报文的判别
	resp->node_id  = server_instance->server_id;	// 向对端发送自己的 node_id 用于身份辨识
	resp->info_type = MICA_REPLICA_UPDATE_RESPONSE;
	resp->info_length = resp_len;
	resp->done_flag = false;						// request_handler 不关心 done_flag（不需要阻塞）

	return resp;
}

static void 
dhmp_mica_update_notify_response_handler(struct dhmp_msg* msg)
{
	struct post_datagram *resp = (struct post_datagram *) (msg->data); 
	struct post_datagram *req = resp->req_ptr; 
	struct dhmp_update_notify_response *resp_info = \
			(struct dhmp_update_notify_response *) DATA_ADDR(resp, 0);

	INFO_LOG("Node [%d] get update response!" , server_instance->server_id);
	resp->req_ptr->done_flag = true;
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
		case MICA_SET_REQUEST:
			INFO_LOG ( "Recv [MICA_SET_REQUEST] from node [%d]",  req->node_id);
			resp = dhmp_mica_set_request_handler(req);
			break;
		case MICA_GET_REQUEST:
			INFO_LOG ( "Recv [MICA_GET_REQUEST] from node [%d]",  req->node_id);
			resp = dhmp_mica_get_request_handler(req);
			break;
		case MICA_REPLICA_UPDATE_REQUEST:
			INFO_LOG ( "Recv [MICA_REPLICA_UPDATE_REQUEST] from node [%d]",  req->node_id);
			resp = dhmp_mica_update_notify_request_handler(req);
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

	// 向主节点返回，将主节点从阻塞状态中返回
	dhmp_post_send(rdma_trans, &res_msg);

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
		case MICA_SET_RESPONSE:
			INFO_LOG ( "Recv [MICA_SET_RESPONSE] from node [%d]",  resp->node_id);
			dhmp_set_response_handler(msg);
			break;
		case MICA_GET_RESPONSE:
			INFO_LOG ( "Recv [MICA_GET_RESPONSE] from node [%d]",  resp->node_id);
			dhmp_get_response_handler(msg);
			break;		
		case MICA_REPLICA_UPDATE_RESPONSE:
			INFO_LOG ( "Recv [MICA_REPLICA_UPDATE_RESPONSE] from node [%d]",  resp->node_id);
			dhmp_mica_update_notify_response_handler(msg);
			break;				
		default:
			break;
	}
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

static struct dhmp_msg *
make_basic_msg(struct dhmp_msg * res_msg, struct post_datagram *resp)
{
	res_msg->msg_type = DHMP_MICA_SEND_INFO_RESPONSE;
	res_msg->data_size = DATAGRAM_ALL_LEN(resp->info_length);
	res_msg->data= resp;
	return res_msg;
}