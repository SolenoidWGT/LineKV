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
#include "nic.h"

#define INIT_DHMP_CLIENT_BUFF_SIZE 1024*1024*8
#define TEST_KV_NUMS    MEHCACHED_ITEMS_PER_BUCKET


struct mehcached_table table_o;
struct mehcached_table main_node_log_table_o;

struct mehcached_table *main_table = &table_o;
struct mehcached_table *log_table = &main_node_log_table_o;

struct replica_mappings * next_node_mappings = NULL;
pthread_t nic_thread;
void* (*main_node_nic_thread_ptr) (void* );
void* (*replica_node_nic_thread_ptr) (void* );
void test_set();

volatile bool replica_is_ready = false;


struct test_kv kvs_group[TEST_KV_NUMS];
static struct test_kv * generate_test_data();
static void free_test_date();
void test_get_consistent(struct test_kv * kvs);

struct ibv_mr * 
mehcached_get_mapping_self_mr(size_t mapping_id)
{
	return &next_node_mappings->mrs[mapping_id];
}

static struct test_kv *
generate_test_data(size_t offset, size_t value_length)
{
    size_t i;
    struct test_kv *kvs_group;
    kvs_group = (struct test_kv *) malloc(sizeof(struct test_kv) * TEST_KV_NUMS);
    memset(kvs_group, 0, sizeof(struct test_kv) * TEST_KV_NUMS);

    for (i = 0; i < TEST_KV_NUMS; i++)
    {
        size_t key = i;
        size_t value = i + offset;
        // uint64_t key_hash = hash((const uint8_t *)&key, sizeof(key));
        value_length = sizeof(value) > value_length ? sizeof(value) : value_length;

        kvs_group[i].true_key_length = sizeof(key);
        kvs_group[i].true_value_length = value_length;
        kvs_group[i].key = (uint8_t *)malloc(kvs_group[i].true_key_length);
        kvs_group[i].value = (uint8_t*) malloc(kvs_group[i].true_value_length);

        memcpy(kvs_group[i].key, &key, kvs_group[i].true_key_length);
        memcpy(kvs_group[i].value, &value, kvs_group[i].true_value_length);
    }

    return kvs_group;
}

static void
free_test_date()
{

}


	// size_t 	 out_value_length; 	// 返回值
	// uint32_t out_expire_time;	// 返回值
	// bool	 partial;			// 返回值
	// uint8_t  out_value[0];		// 返回值

// 我们不回去比较key，因为如果value可以正确拿到，则key一定是正确的（另外我们没用拿key的接口)
bool 
cmp_item_value(size_t a_value_length, const uint8_t *a_out_value, size_t b_value_length,const uint8_t *b_out_value)
{
    bool re = true;
    if (a_value_length != b_value_length)
    {
        ERROR_LOG("MICA value length error! %lu != %lu", a_value_length, b_value_length);
        re= (false);
    }

    if (memcmp(a_out_value, b_out_value, b_value_length) != 0 )
    {
        ERROR_LOG("value context error! %p, %p, len is %lu", a_out_value, b_out_value, b_value_length);
        re=  (false);
    }

    // if (memcmp(GET_TRUE_VALUE_ADDR(a_out_value), GET_TRUE_VALUE_ADDR(b_out_value), GET_TRUE_VALUE_LEN(b_value_length)) != 0 )
    // {
    //     ERROR_LOG("true value error!");
    //     re=  (false);
    // }
    return re;
}


bool 
cmp_item_all_value(size_t a_value_length, const uint8_t *a_out_value, size_t b_value_length,const uint8_t *b_out_value)
{
    bool re = true;
    if (a_value_length != b_value_length)
    {
        ERROR_LOG("MICA value length error! %lu != %lu", a_value_length, b_value_length);
        re= (false);
    }

    if (memcmp(a_out_value, b_out_value, b_value_length) != 0 )
    {
        ERROR_LOG("value context error! %p, %p, len is %lu", a_out_value, b_out_value, b_value_length);
        // struct midd_value_header
        // {
        //     uint64_t version;
        //     uint64_t value_count;
        //     uint8_t data[0];
        // };

        // struct midd_value_tail
        // {
        //     uint64_t version;
        //     volatile bool dirty;     // 我们将脏标志位放在value末尾，最后被更新
        // };
        // dump_value_by_addr(a_out_value, a_value_length);
        // dump_value_by_addr(b_out_value, b_value_length);
        re=  (false);
    }

    // if (memcmp(GET_TRUE_VALUE_ADDR(a_out_value), GET_TRUE_VALUE_ADDR(b_out_value), GET_TRUE_VALUE_LEN(b_value_length)) != 0 )
    // {
    //     ERROR_LOG("true value error!");
    //     re=  (false);
    // }
    return re;
}


void
test_set(struct test_kv * kvs, size_t val_offset)
{
    INFO_LOG("---------------------------test_set()---------------------------");
    size_t i;
    struct set_requset_pack *req_callback_ptr = (struct set_requset_pack *)\
            malloc(sizeof(struct set_requset_pack) * server_instance->node_nums);
                 
    for (i = 0; i < TEST_KV_NUMS; i++)
    {
        // struct mehcached_item * item;
        bool re;
        const uint8_t* key = kvs[i].key;
        size_t new_value = i + val_offset;
        memcpy(kvs_group[i].value, &new_value, kvs_group[i].true_value_length);  // 更新value
        const uint8_t* value = kvs[i].value;
        size_t true_key_length = kvs[i].true_key_length;
        size_t true_value_length = kvs[i].true_value_length;
        uint64_t key_hash = hash(key, true_key_length);

        re = mehcached_set(0, main_table, key_hash,\
                            key, true_key_length, \
                            value, true_value_length, 0, true);

        // Assert(is_update == false);
        if (!re)
        {
            ERROR_LOG("Main node set fail! keyhash is %lx", key_hash);
            exit(0);
        }

        // 镜像节点全value赋值
        // 下方到网卡进行远端复制
        makeup_update_request(key, true_key_length, value, true_value_length);
    }

    free(req_callback_ptr);
    // mehcached_table_free(main_table);
    INFO_LOG("---------------------------test_set finished!---------------------------");
    // 主线程等待1s，让输出更清晰一点
    sleep(1);
    test_get_consistent(kvs);
}

int main()
{
    // 初始化集群 rdma 连接
    size_t numa_nodes[] = {(size_t)-1};;
    const size_t page_size = 1048576 * 2;
	const size_t num_numa_nodes = 2;
    const size_t num_pages_to_try = 16384;
    const size_t num_pages_to_reserve = 16384 - 2048;   // give 2048 pages to dpdk

    // 初始化 hook
    set_main_node_thread_addr(&main_node_nic_thread_ptr);
    set_replica_node_thread_addr(&replica_node_nic_thread_ptr);

    // 初始化本地存储，分配 page
	mehcached_shm_init(page_size, num_numa_nodes, num_pages_to_try, num_pages_to_reserve);

    // 初始化 rdma 连接
    server_instance = dhmp_server_init();
    client_mgr = dhmp_client_init(INIT_DHMP_CLIENT_BUFF_SIZE);
    Assert(server_instance);
    Assert(client_mgr);
    INFO_LOG("---------------------------RDMA server or client init finish!------------------------------");


    next_node_mappings = (struct replica_mappings *) malloc(sizeof(struct replica_mappings));
    memset(next_node_mappings, 0, sizeof(struct replica_mappings));

    if (IS_MAIN(server_instance->server_type))
    {
        if (server_instance->node_nums < 3)
        {
            ERROR_LOG("The number of cluster is not enough");
            exit(0);
        }
        Assert(server_instance->server_id == 0);
    }


    mehcached_table_init(main_table, 1, 1, 256, false, false, false,\
            numa_nodes[0], numa_nodes, MEHCACHED_MTH_THRESHOLD_FIFO);
    Assert(main_table);

    // 主节点初始化远端hash表，镜像节点初始化自己本地的hash表
	if (IS_MAIN(server_instance->server_type))
    {
		MID_LOG("Node [%d] do MAIN node init work", server_instance->server_id);
        // 向所有副本节点发送 main_table 初始化请求
		Assert(server_instance->server_id == MAIN_NODE_ID);
		size_t nid;
		size_t init_nums = 0;
		bool* inited_state = (bool *) malloc(sizeof(bool) * server_instance->node_nums);
		memset(inited_state, false, sizeof(bool) * server_instance->node_nums);

		// for (nid = REPLICA_NODE_HEAD_ID; nid <= REPLICA_NODE_TAIL_ID; nid++)
		// 等待全部节点初始化完成,才能开放服务
		// 因为是主节点等待，所以主节点主动去轮询，而不是被动等待从节点发送完成信号
		while (init_nums < REPLICA_NODE_NUMS)
		{
			enum ack_info_state ack_state;
            init_nums = 0;
			for (nid = 1; nid <= REPLICA_NODE_TAIL_ID; nid++) 
			{
				if (inited_state[nid] == false)
				{
					ack_state = mica_basic_ack_req(nid, MICA_INIT_ADDR_ACK, true);
					if (ack_state == MICA_ACK_INIT_ADDR_OK)
					{
						inited_state[nid] = true;
						init_nums++;
					}
                    else
                    {
                        // retry
                        if (ack_state != MICA_ACK_INIT_ADDR_NOT_OK)
                        {
                            ERROR_LOG("replica node [%d] unkown error!", nid);
                            Assert(false);
                        }
                    }
				}
                else
                    init_nums++;
			}
		}

        // 主节点也要启动网卡线程
        pthread_create(&nic_thread, NULL, *replica_node_nic_thread_ptr, NULL);
		INFO_LOG("---------------------------MAIN node init finished!------------------------------");
        
        // 主节点启动测试程序
        struct test_kv * kvs = generate_test_data(0, sizeof(size_t));
        test_set(kvs, 0);
        test_set(kvs, 100);
        test_set(kvs, 1000);
    }

    // 存在下游节点的副本节点向下游节点发送初始化请求
	if(IS_REPLICA(server_instance->server_type))
    {
        // 各个副本节点使用 主线程 检查是否已经和 主节点，上游节点 建立连接
        // 副本节点是 server ， 主节点和上游节点是 client
        struct dhmp_transport *rdma_trans=NULL;
        int expected_connections;
        int active_connection = 0;
        size_t up_node;
    
        if (!IS_TAIL(server_instance->server_type))
        {
            // 启动网卡线程
            pthread_create(&nic_thread, NULL, *replica_node_nic_thread_ptr, NULL);
            MID_LOG("Node [%d] is started nicthread and get cliMR!", server_instance->server_id);
        }

        if (server_instance->server_id == REPLICA_NODE_HEAD_ID)
            up_node = 0;    // 链表中的头节点没用上游节点
        else
            up_node = server_instance->server_id-1;

        Assert(up_node != (size_t)-1);

        if (server_instance->server_id == REPLICA_NODE_HEAD_ID)
            expected_connections = 1;  // 链表中的头节点只需要被动接受主节点的连接
        else
            expected_connections = 2;  // 非头节点需要被动接受主节点和上游节点的连接

        replica_is_ready = true;

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
                        // 而对于dhmp服务端来说只能使用 rpc 的方式确定每一个 trans 所对应的 peer_nodeid
                        int peer_id = mica_ask_nodeID_req(rdma_trans);
                        if (peer_id == -1)
                        {
                            pthread_mutex_unlock(&server_instance->mutex_client_list);
                            break;
                        }
                        active_connection++;
                        rdma_trans->node_id = peer_id;
                    }
                    INFO_LOG("Replica node [%d] get \"dhmp client\" (MICA MAIN node or upstream node) id is [%d], success!", \
                                server_instance->server_id, rdma_trans->node_id);
                }
            }
            pthread_mutex_unlock(&server_instance->mutex_client_list);

            if (active_connection == expected_connections)
                break;
        }
    }

    pthread_join(server_instance->ctx.epoll_thread, NULL);
    return 0;
}


// 测试所有节点中的数据必须一致
void test_get_consistent(struct test_kv * kvs)
{
    size_t i, nid;
    INFO_LOG("---------------------------test_get_consistent!---------------------------");
    for (i = 0; i < TEST_KV_NUMS; i++)
    {
        const uint8_t* key = kvs[i].key;
        const uint8_t* value = kvs[i].value;
        size_t true_key_length = kvs[i].true_key_length;
        size_t true_value_length = kvs[i].true_value_length;
        uint64_t key_hash = hash(key, true_key_length);
        uint8_t* out_value = (uint8_t*)malloc(true_value_length);
        size_t  out_value_length = 99999;
        uint32_t expire_time;
        struct dhmp_mica_get_response *get_result = NULL;

        // mehcached_get(current_alloc_id, table, key_hash, key, 
        //                             key_length + KEY_HEADER_LEN, 
        //                             out_value, 
        //                             in_out_value_length, 
        //                             out_expire_time, readonly, get_true_value);
        // 测试本地 table 数据一致
        if (!mehcached_get(0, main_table, key_hash, key, true_key_length,\
                                     out_value, &out_value_length, \
                                     &expire_time, false))
        {
            ERROR_LOG("key hash [%lx] get false", key_hash);
            Assert(false);
        }

        if (!cmp_item_value(true_value_length, value, out_value_length, out_value))
        {
            ERROR_LOG("local item key_hash [%lx] value compare false!", key_hash);
            Assert(false);
        }
        INFO_LOG("No.<%d> Main Node [%d] set test success!", i, MAIN_NODE_ID);

        // // 测试镜像节点数据一致
        // get_result = mica_get_remote_warpper(0, key_hash, key, true_key_length, false, NULL, MIRROR_NODE_ID);
        // if (get_result == NULL || get_result->out_value_length == (size_t) - 1)
        // {
        //     ERROR_LOG("MICA get key %lx failed!", key_hash);
        //     Assert(false);
        // }
        // if (get_result->partial == true)
        // {
        //     ERROR_LOG("value too long!");
        //     Assert(false);
        // }
        // if (!cmp_item_value(get_result->out_value_length, get_result->out_value, out_value_length, out_value))
        // {
        //     ERROR_LOG("Mirror item key_hash [%lx] value compare false!", key_hash);
        //     Assert(false);
        // }
        // free(get_result);
        // INFO_LOG("No.<%d>, Mirror Node [%d] set test success!", i, MIRROR_NODE_ID);

       // 测试远端 replica 节点数据一致
        for (nid = 1; nid <= REPLICA_NODE_TAIL_ID; nid++)
        {
            // 如果 version 不一致，需要反复尝试get直到一致后才会返回结果
            while(true)
            {
                // 远端获取的默认是带header和tailer的value
                get_result = mica_get_remote_warpper(0, key_hash, key, true_key_length, false, NULL, nid);
                if (get_result == NULL)
                {
                    ERROR_LOG("MICA get key %lx failed!", key_hash);
                    Assert(false);
                }
                
                if (get_result->out_value_length != (size_t) - 1)
                    break;
            }
 
            if (get_result->partial == true)
            {
                ERROR_LOG("value too long!");
                Assert(false);
            }

            if (!cmp_item_value(get_result->out_value_length, get_result->out_value, true_value_length, value))
            {
                ERROR_LOG("Replica node [%d] item key_hash [%lx] value compare false!", nid, key_hash);
                Assert(false);
            }
            INFO_LOG("No.<%d> Replica Node [%d] set test success!",i, nid);
            free(get_result);
        }

        INFO_LOG("No.<%d> Key_hash [%lx] pas all compare scuess!", i, key_hash);
    }
    INFO_LOG("---------------------------test_get_consistent finish!---------------------------");
}