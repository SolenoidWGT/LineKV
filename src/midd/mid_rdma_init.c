#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <arpa/inet.h>

#include "dhmp.h"
#include "dhmp_transport.h"
#include "dhmp_server.h"
#include "dhmp_dev.h"
#include "dhmp_client.h"
#include "dhmp_log.h"

struct dhmp_server *server_instance=NULL;
struct dhmp_client *client_mgr=NULL;
size_t CLINET_ID=(size_t)-1;
static struct dhmp_send_mr * init_read_mr(int buffer_size, struct ibv_pd* pd);

struct dhmp_device *dhmp_get_dev_from_client()
{
	struct dhmp_device *res_dev_ptr=NULL;
	if(!list_empty(&client_mgr->dev_list))
	{
		res_dev_ptr=list_first_entry(&client_mgr->dev_list,
									struct dhmp_device,
									dev_entry);
	}
		
	return res_dev_ptr;
}

/**
 *	dhmp_get_dev_from_server:get the dev_ptr from dev_list of server_instance.
 */
struct dhmp_device *dhmp_get_dev_from_server()
{
	struct dhmp_device *res_dev_ptr=NULL;
	if(!list_empty(&server_instance->dev_list))
	{
		res_dev_ptr=list_first_entry(&server_instance->dev_list,
									struct dhmp_device,
									dev_entry);
	}
		
	return res_dev_ptr;
}

static struct dhmp_send_mr * 
init_read_mr(int buffer_size, struct ibv_pd* pd)
{
	struct dhmp_send_mr * rd_mr = malloc(sizeof(struct dhmp_send_mr));
	void* tmp_buf = malloc(buffer_size);

	memset(tmp_buf, 0, buffer_size);
	rd_mr->mr = ibv_reg_mr(pd, tmp_buf, buffer_size,
										IBV_ACCESS_LOCAL_WRITE);
	return rd_mr;
}


struct dhmp_transport * 
dhmp_connect(int peer_node_id)
{
	struct dhmp_transport * conn = NULL;
	INFO_LOG("create the [%d]-th normal transport.",peer_node_id);

	while(1)
	{
		conn = dhmp_transport_create(&client_mgr->ctx, 
								dhmp_get_dev_from_client(),		/* device 这里需要考虑下多个节点的情况吗？ 我认为不用，因为一个node只有一个RDMA设备*/
								false,
								false,
								peer_node_id);
		if(!conn)
		{
			ERROR_LOG("create the [%d]-th transport error.", peer_node_id);
			return NULL;
		}

		dhmp_transport_connect(conn,
								client_mgr->config.net_infos[peer_node_id].addr,
								client_mgr->config.net_infos[peer_node_id].port);

		/* main thread sleep a while, wait dhmp_event_channel_handler finish connection*/
		// sleep(1);
		// sleep_ms(100);

		while(conn->trans_state < DHMP_TRANSPORT_STATE_CONNECTED)
			sleep_ms(100);

		if(conn->trans_state == DHMP_TRANSPORT_STATE_REJECT)
		{
			free_trans(conn);
			free(conn);
		}
		else if(conn->trans_state == DHMP_TRANSPORT_STATE_ADDR_ERROR)
		{
			free_trans(conn);
			free(conn);
		}
		else if(conn->trans_state == DHMP_TRANSPORT_STATE_CONNECTED )
		{
			conn->is_active = true;
			// struct ibv_port_attr port_info;
			// int re = ibv_query_port(dhmp_get_dev_from_client()->verbs, ntohs(rdma_get_src_port(conn->cm_id)), &port_info);
			// if (re == -1)
			// 	ERROR_LOG("retrieves ibv_query_port error!");
			// else
			// 	INFO_LOG("Client port [%u] max message legnth is [%u].",ntohs(rdma_get_src_port(conn->cm_id)), port_info.max_msg_sz);
			break;
		}
	}

	DEBUG_LOG("CONNECT finished: Peer Server %d has been connnected!", peer_node_id);
	return conn;
}

struct dhmp_client *  dhmp_client_init(size_t buffer_size, bool is_mica_cli)
{
	int i;
	int re = 0;
	struct dhmp_device * cli_pd;

	client_mgr=(struct dhmp_client *)malloc(sizeof(struct dhmp_client));
	memset(client_mgr, 0 , sizeof(struct dhmp_client));
	for (i=0; i<MAX_CQ_NUMS; i++)
		client_mgr->ctx.stop_flag[i] = true;

	if(!client_mgr)
	{
		ERROR_LOG("alloc memory error.");
		return NULL;
	}

	if (!is_mica_cli)
	{
		// 我们这里直接使用 server 创建的 config 结构体，而不是自己去初始化
		memcpy(&client_mgr->config, &server_instance->config, sizeof(struct dhmp_config));
		
	}
	else
	{
		// 是 mica 客户端则正常初始化
		dhmp_config_init(&client_mgr->config, true);
	}

	re = dhmp_context_init(&client_mgr->ctx);

	/*init list about rdma device*/
	INIT_LIST_HEAD(&client_mgr->dev_list);
	dhmp_dev_list_init(&client_mgr->dev_list);

	/*init FIFO node select algorithm*/
	client_mgr->fifo_node_index=0;

	/*init the addr hash table of client_mgr*/
	for(i=0;i<DHMP_CLIENT_HT_SIZE;i++)
	{
		INIT_HLIST_HEAD(&client_mgr->addr_info_ht[i]);
	}

	/*init the structure about send mr list */
	pthread_mutex_init(&client_mgr->mutex_send_mr_list, NULL);
	INIT_LIST_HEAD(&client_mgr->send_mr_list);

	/*init normal connection*/
	memset(client_mgr->connect_trans, 0, DHMP_SERVER_NODE_NUM*
										sizeof(struct dhmp_transport*));

	// 客户端主动和主节点建立连接 
	if (!is_mica_cli)
	{
#ifndef CRAQ
		if(IS_MAIN(server_instance->server_type))
		{
			// 头节点需要主动和所有的node建立rdma连接，所有的node都是头节点的server
			for(i=0; i<client_mgr->config.nets_cnt; i++)
			{
				/*server_instance skip himself to avoid connecting himself*/
				if(server_instance->server_id == i)
				{
					client_mgr->self_node_id = i;
					client_mgr->connect_trans[i] = NULL;
					continue;
				}

				INFO_LOG("CONNECT BEGIN: create the [%d]-th normal transport.",i);
				client_mgr->connect_trans[i] = dhmp_connect(i);
				if(!client_mgr->connect_trans[i])
				{
					ERROR_LOG("create the [%d]-th transport error.",i);
					continue;
				}
				client_mgr->connect_trans[i]->is_active = true;
				client_mgr->connect_trans[i]->node_id = i;
				client_mgr->read_mr[i] = init_read_mr(buffer_size, client_mgr->connect_trans[i]->device->pd);
			}
		}

		// 排除集群中只有一个副本节点的情况
		if(IS_REPLICA(server_instance->server_type) && 
				server_instance->node_nums > 3 &&
				server_instance->server_id != server_instance->node_nums-1)
#else
		if(IS_REPLICA(server_instance->server_type) && 
			server_instance->server_id != server_instance->node_nums-1)
#endif
		{
			// 中间节点需要主动和下游节点建立rdma连接，只有下游节点是中间节点的server
			int next_id = server_instance->server_id+1;
			client_mgr->connect_trans[next_id] = dhmp_connect(next_id);

			if(!client_mgr->connect_trans[next_id]){
				ERROR_LOG("create the [%d]-th transport error.",next_id);
				exit(0);
			}

			client_mgr->connect_trans[next_id]->is_active = true;
			client_mgr->connect_trans[next_id]->node_id = next_id;
			client_mgr->read_mr[next_id] = init_read_mr(buffer_size, client_mgr->connect_trans[next_id]->device->pd);	
		}
	}

	/* 初始化client段全局对象 */
	// global_verbs_send_mr = (struct dhmp_send_mr* )malloc(sizeof(struct dhmp_send_mr));

	/*init the structure about work thread*/
	pthread_mutex_init(&client_mgr->mutex_work_list, NULL);
	pthread_mutex_init(&client_mgr->mutex_asyn_work_list, NULL);
	INIT_LIST_HEAD(&client_mgr->work_list);
	INIT_LIST_HEAD(&client_mgr->work_asyn_list);

	return client_mgr;
}

struct dhmp_server * dhmp_server_init(size_t server_id)
{
	int i,err=0;
	struct ibv_port_attr port_info;
	uint16_t port_num, phys_port_cnt;
	int re;

	memset((void*)used_id, -1, sizeof(int) * MAX_PORT_NUMS);
	server_instance=(struct dhmp_server *)malloc(sizeof(struct dhmp_server));
	if(!server_instance)
	{
		ERROR_LOG("allocate memory error.");
		return NULL;
	}

	for (i=0; i<MAX_CQ_NUMS; i++)
		server_instance->ctx.stop_flag[i] = true;

	dhmp_hash_init();
	dhmp_config_init(&server_instance->config, false);
	dhmp_context_init(&server_instance->ctx);
	// server_instance->server_id = server_instance->config.curnet_id;
	server_instance->config.curnet_id = server_id;
	server_instance->server_id = server_id;
	server_instance->node_nums = server_instance->config.nets_cnt;
	Assert((server_id != ((size_t) -1) && server_id < server_instance->node_nums));

	// 所有的主节点都需要拥有多线程能力
	init_mulit_server_work_thread();

	/*init client transport list*/
	server_instance->cur_connections=0;
	pthread_mutex_init(&server_instance->mutex_client_list, NULL);
	INIT_LIST_HEAD(&server_instance->client_list);

	/*init list about rdma device*/
	INIT_LIST_HEAD(&server_instance->dev_list);
	dhmp_dev_list_init(&server_instance->dev_list);

	server_instance->listen_trans=dhmp_transport_create(&server_instance->ctx,
											dhmp_get_dev_from_server(),
											true, false, -2);
	if(!server_instance->listen_trans)
	{
		ERROR_LOG("create rdma transport error.");
		exit(-1);
	}

	// TODO：从外部命令行输入节点ID，不需要使用循环争抢节点ID
	while (1)
	{
		err=dhmp_transport_listen(server_instance->listen_trans,
				server_instance->config.net_infos[server_instance->config.curnet_id].port);

		if (err == 0)
		{
			INFO_LOG("Final curnet_id is %d, port is %u", server_instance->config.curnet_id, \
					(unsigned int)server_instance->config.net_infos[server_instance->config.curnet_id].port);

			server_instance->server_id = server_instance->config.curnet_id;
#ifndef CRAQ
			if (server_instance->config.nets_cnt < 3)
			{
				ERROR_LOG("Too few nodes to start system, at least node num is [3], now is [%d], exit!", \
						server_instance->config.nets_cnt);
				exit(0);
			}

			if (server_instance->server_id == 0)
				SET_MAIN(server_instance->server_type);
			else if (server_instance->server_id == 1)
				SET_MIRROR(server_instance->server_type);
			else
				SET_REPLICA(server_instance->server_type);
			
			// 尾节点单独 set 标志位
			if (server_instance->server_id == server_instance->node_nums - 1)
			{
				SET_TAIL(server_instance->server_type);
				INFO_LOG("Tail Node server_id is [%d] ", server_instance->server_id);
			}
			
			// 非主节点的头副本节点
			if (server_instance->server_id == 2)
				SET_HEAD(server_instance->server_type);
#else
			// 所有节点都是副本节点
			SET_REPLICA(server_instance->server_type);

			if (server_instance->server_id == 0)
			{
				// SET_MAIN(server_instance->server_type);
				SET_HEAD(server_instance->server_type);
			}

			// 尾节点单独 set 标志位
			if (server_instance->server_id == server_instance->node_nums - 1)
			{
				SET_TAIL(server_instance->server_type);
				INFO_LOG("Tail Node server_id is [%d] ", server_instance->server_id);
			}

#endif
			MID_LOG("Server's node id is [%d], node_nums is [%d], server_type is %d", \
					server_instance->server_id, server_instance->node_nums, server_instance->server_type);
			break;
		}
		else
		{
			used_id[used_nums++] = server_instance->config.curnet_id;
			dhmp_set_curnode_id ( &server_instance->config );
		}
	}

	// 输出 rdma 设备信息
	phys_port_cnt = dhmp_get_dev_from_server()->device_attr.phys_port_cnt;
	INFO_LOG("server total phys_port_cnt is [%d].", phys_port_cnt);
	for (port_num = 1; port_num <= phys_port_cnt; port_num++) 
	{
		re = ibv_query_port(dhmp_get_dev_from_server()->verbs, port_num , &port_info);
		if (re) {
			fprintf(stderr, "Error, failed to query port %d attributes in device '%s'",
				port_num, ibv_get_device_name(dhmp_get_dev_from_server()->verbs->device));
			return NULL;
		}
		else
			INFO_LOG("Server port [%u] max message legnth is [%u] MB.", port_num, port_info.max_msg_sz / (1024 * 1024));
	}

	return server_instance;
}

int mica_clinet_connect_server(int buffer_size, int target_id)
{
	// 目前测试客户端只与主节点连接
	int idx = client_mgr->conn_index;
	INFO_LOG("CONNECT BEGIN: create the [%d]-th normal transport.",target_id);
	client_mgr->connect_trans[idx] = dhmp_connect(target_id);	// 目前测试客户端只与主节点连接
	if(!client_mgr->connect_trans[idx])
	{
		ERROR_LOG("create the [%d]-th transport error.",target_id);
		return -1;
	}
	client_mgr->connect_trans[idx]->is_active = true;
	client_mgr->connect_trans[idx]->node_id = target_id;
	client_mgr->read_mr[idx] = init_read_mr(buffer_size, client_mgr->connect_trans[idx]->device->pd);
	client_mgr->conn_index++;
	Assert(client_mgr->conn_index < DHMP_SERVER_NODE_NUM);
	return 1;
}

void dhmp_server_destroy()
{
	INFO_LOG("server_instance destroy start.");
	pthread_join(server_instance->ctx.epoll_thread, NULL);
	int err = 0;
	// err = memkind_destroy_kind(pmem_kind);
    //     if(err)
    //     {
    //             ERROR_LOG("memkind_destroy_kind() error.");
    //             return;
    //     }
		
	INFO_LOG("server_instance destroy end.");
	free(server_instance);
}


