#include "shm_private.h"
#include "dhmp_server.h"
#include "dhmp.h"
#include "dhmp_log.h"
#include "dhmp_top_api.h"
#ifdef USE_RDMA

// mehcached_node_init 函数被执行时，节点自身的内存空间已经注册完成
void
mehcached_node_init()
{
	if (IS_MAIN(server_instance->server_type))
    {
		MID_LOG("Node [%d] do MAIN node init work", server_instance->server_id);
        // 向所有副本节点发送 table 初始化请求
		Assert(server_instance->server_id == MAIN_NODE_ID ||
				server_instance->server_id == MIRROR_NODE_ID);
		size_t nid;
		size_t init_nums = 0;
		size_t node_nums = (size_t) server_instance->config.nets_cnt;
		struct replica_mappings * node_mappings = (struct replica_mappings *) \
						malloc(sizeof(struct replica_mappings) * node_nums);
		bool* inited_state = (bool *) malloc(sizeof(bool) * server_instance->node_nums);

		memset(node_mappings, 0, sizeof(struct replica_mappings) * node_nums);
		memset(inited_state, 0, sizeof(bool) * server_instance->node_nums);

		for (nid = REPLICA_NODE_HEAD_ID; nid <= REPLICA_NODE_TAIL_ID; nid++)
			micaserver_get_cliMR(&node_mappings[nid], nid);

		// 等待全部节点初始化完成,才能开放服务
		// 因为是主节点等待，所以主节点主动去轮询，而不是被动等待从节点发送完成信号
		while (init_nums < REPLICA_NODE_NUMS)
		{
			enum ack_info_state ack_state;
			for (nid = REPLICA_NODE_HEAD_ID; nid <= REPLICA_NODE_TAIL_ID; nid++) 
			{
				if (inited_state[nid] == false)
				{
					ack_state = mica_basic_ack_req(nid, MICA_INIT_ADDR_ACK);
					if (ack_state == MICA_ACK_INIT_ADDR_OK)
					{
						inited_state[nid] = true;
						init_nums++;
					}
				}
			}
		}
		INFO_LOG("MAIN node: all replica node is inited");
    }

	if (IS_MIRROR(server_instance->server_type))
	{
		// 镜像节点只需要负责初始化自己的hash表即可，不需要知道副本节点的存储地址
		MID_LOG("Node [%d] is mirror node, don't do any init work", server_instance->server_id);
	}

	if(IS_REPLICA(server_instance->server_type) &&
			server_instance->node_nums > 3 &&
			server_instance->server_id != server_instance->node_nums -1)
    {
		MID_LOG("Node [%d] is REPLICA-MIDDLE node, start to connect downstram node", server_instance->server_id);
        // 向下游节点发送 table 初始化请求
		struct replica_mappings * sub_node_mapping = \
							(struct replica_mappings *) malloc(sizeof(struct replica_mappings));
		size_t target_id = server_instance->server_id + 1;
		Assert(target_id != server_instance->node_nums);

		memset(sub_node_mapping, 0, sizeof(struct replica_mappings));
		micaserver_get_cliMR(sub_node_mapping, target_id);
    }
}
#endif
MEHCACHED_END

