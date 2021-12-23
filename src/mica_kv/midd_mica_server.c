#include "mehcached.h"
#include "hash.h"


#include "dhmp.h"
#include "dhmp_log.h"
#include "dhmp_hash.h"
#include "dhmp_config.h"
#include "dhmp_context.h"
#include "dhmp_dev.h"
#include "dhmp_transport.h"
#include "dhmp_task.h"

#include "dhmp_client.h"
#include "dhmp_server.h"
#include "dhmp_init.h"
#include "mid_rdma_utils.h"
#include "dhmp_top_api.h"
#define INIT_DHMP_CLIENT_BUFF_SIZE 1024*1024*8

struct mehcached_table table_o;
struct mehcached_table *table = &table_o;
int main()
{
    // 初始化集群 rdma 连接
    size_t numa_nodes[] = {(size_t)-1};;
    const size_t page_size = 1048576 * 2;
	const size_t num_numa_nodes = 2;
    const size_t num_pages_to_try = 16384;
    const size_t num_pages_to_reserve = 16384 - 2048;   // give 2048 pages to dpdk

    // 初始化本地存储，分配 page
	mehcached_shm_init(page_size, num_numa_nodes, num_pages_to_try, num_pages_to_reserve);

    // 初始化 rdma 连接
    server_instance = dhmp_server_init();
    client_mgr = dhmp_client_init(INIT_DHMP_CLIENT_BUFF_SIZE);
    Assert(server_instance);
    Assert(client_mgr);

    if (IS_MAIN(server_instance->server_type))
    {
        if (server_instance->node_nums < 3)
        {
            ERROR_LOG("The number of cluster is not enough");
            exit(0);
        }
        Assert(server_instance->server_id == 0);
    }

    // 主节点和镜像节点初始化本地hash表
    if (IS_MAIN(server_instance->server_type) || 
        IS_MIRROR(server_instance->server_type))
    {
        mehcached_table_init(table, 1, 1, 256, false, false, false,\
             numa_nodes[0], numa_nodes, MEHCACHED_MTH_THRESHOLD_FIFO);
        Assert(table);
    }

    // 主节点初始化远端hash表，镜像节点初始化自己本地的hash表
    mehcached_node_init();

    if (IS_MAIN(server_instance->server_type))
        INFO_LOG("MAIN node init finished!");
    else if (IS_MIRROR(server_instance->server_type))
        INFO_LOG("MIRROR node init finished!");
    else
    {
        // 各个副本节点使用 主线程 检查是否已经和 主节点，上游节点 建立连接
        // 副本节点是 server ， 主节点和上游节点是 client
        struct dhmp_transport *rdma_trans=NULL;
        int expected_connections;
        int active_connection = 0;
        size_t up_node;

        if (server_instance->server_id == 2)
            up_node = 0;    // 链表中的头节点没用上游节点
        else
            up_node = server_instance->server_id-1;

        Assert(up_node != (size_t)-1);

        if (server_instance->server_id == 2)
            expected_connections = 1;  // 链表中的头节点只需要被动接受主节点的连接
        else
            expected_connections = 2;  // 非头节点需要被动接受主节点和上游节点的连接

        while (true)
        {
            pthread_mutex_lock(&server_instance->mutex_client_list);
            if (server_instance->cur_connections == expected_connections)
            {
                list_for_each_entry(rdma_trans, &server_instance->client_list, client_entry)
                {
                    if (rdma_trans->node_id == -1)
                    {
                        // 由于只有dhmp客户端可以在建立连接的时候显式的标记 dhmp_trans 的 peer_nodeid
                        // 而对于dhmp服务端来说只能使用rpc的方式确定每一个trans 所对应的 peer_nodeid
                        int peer_id = mica_ask_nodeID_req(rdma_trans);
                        if (peer_id == -1)
                        {
                            pthread_mutex_unlock(&server_instance->mutex_client_list);
                            break;
                        }
                        active_connection++;
                        rdma_trans->node_id = peer_id;
                    }
                    INFO_LOG("Replica node [%d] get \"dhmp client\" (MICA MAIN node or upstream node) id is [%d], sucess!", \
                                server_instance->server_id, rdma_trans->node_id);
                }
            }
            pthread_mutex_unlock(&server_instance->mutex_client_list);

            if (active_connection == expected_connections)
                break;
        }

        while(get_table_init_state() == false);
        INFO_LOG("Replica Node [%d] init finished!", server_instance->server_id);
    }

    pthread_join(server_instance->ctx.epoll_thread, NULL);
    return 0;
}