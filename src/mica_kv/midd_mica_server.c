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

#include "midd_mica_benchmark.h"
#include "mica_partition.h"

void new_main_test_through();

pthread_t nic_thread[PARTITION_MAX_NUMS];
void* (*main_node_nic_thread_ptr) (void* );
void* (*replica_node_nic_thread_ptr) (void* );
void test_set(struct test_kv * kvs);

struct dhmp_msg* all_access_set_group;

int *partition_req_count_array;

void generate_local_get_mgs();

int main(int argc,char *argv[])
{
    // 初始化集群 rdma 连接
    int i, retval;
    int nic_thread_num;
    size_t numa_nodes[] = {(size_t)-1};;
    const size_t page_size = 4*1024UL;
	const size_t num_numa_nodes = 2;
    const size_t num_pages_to_try = 16384;
    const size_t num_pages_to_reserve = 16384 - 2048;   // give 2048 pages to dpdk
    
    INFO_LOG("Server argc is [%d]", argc);
    main_node_is_readable = false;
    Assert(argc==8);
    for (i = 0; i<argc; i++)
	{
        if (i==1)
        {
            SERVER_ID = (size_t)(*argv[i] - '0');
            INFO_LOG("Server node_id is [%d]", SERVER_ID);
        }
        else if (i==2)
        {
            __partition_nums = (unsigned long long) atoi(argv[i]);
            Assert(__partition_nums >0 && __partition_nums < PARTITION_MAX_NUMS);
            INFO_LOG("Server __partition_nums is [%d]", __partition_nums);
        }
        else if (i==3)
        {
            is_ubuntu = atoi(argv[i]);
            INFO_LOG("Server is_ubuntu is [%d]", is_ubuntu);
        }
        else if (i==4)
        {
            __test_size = atoi(argv[i]);
            INFO_LOG(" __test_size is [%d]", __test_size);
        }
        else if (i==5)
        {
            if (strcmp(argv[i], "uniform") == 0)
            {
                INFO_LOG(" workload_type is [%s]", argv[i]);
                workload_type=UNIFORM;
            }
            else if (strcmp(argv[i], "zipfian") == 0)
            {
                INFO_LOG(" workload_type is [%s]", argv[i]);
                workload_type=ZIPFIAN;
            }
            else
            {
                ERROR_LOG("Unkown workload!");
                exit(0);
            }
        }
        else if (i==6)
        {
            __access_num = atoi(argv[i]);
            INFO_LOG(" __access_num is [%d]", __access_num);
        }
        else if (i==7)
        {
            // get : set
            // 写比读多好处理
            if(strcmp(argv[i], "0.5") == 0)
            {
                INFO_LOG(" RW_TATE is [%s]", argv[i]);
                // read_num = ACCESS_NUM /2;
                // update_num = ACCESS_NUM /2;
                read_num = 5;
                update_num = 5;
            }
            else if(strcmp(argv[i], "1.0") == 0)
            {
                INFO_LOG(" RW_TATE is [%s]", argv[i]);
                read_num = 0;
                update_num = 6000;
                get_is_more = false;
                is_all_set_all_get = true;
            }
            else if(strcmp(argv[i], "0.75") == 0)
            {
                INFO_LOG(" RW_TATE is [%s]", argv[i]);
                read_num = 1;
                update_num =3;
            }
            // 读比写多比较麻烦，需要各节点自己自动执行写操作
            // 最极端的情况是全读
            // 为了能触发和同步各节点的读操作，我们至少要设置一个写操作，并第一个执行
            else if(strcmp(argv[i], "0.0") == 0)
            {
                INFO_LOG(" RW_TATE is [%s]", argv[i]);
                read_num = 6000;
                update_num = 1;     // 至少有一个写操作
                get_is_more = true;
                is_all_set_all_get = true;
            }
            else if(strcmp(argv[i], "0.2") == 0)
            {
                INFO_LOG(" RW_TATE is [%s]", argv[i]);
                read_num = 4;
                update_num = 1;
            }
            else if(strcmp(argv[i], "0.01") == 0)
            {
                INFO_LOG(" RW_TATE is [%s]", argv[i]);
                read_num = 99;
                update_num = 1;
                main_node_is_readable = true;
            }
            else if(strcmp(argv[i], "0.05") == 0)
            {
                INFO_LOG(" RW_TATE is [%s]", argv[i]);
                read_num = 19;
                update_num = 1;
                main_node_is_readable = true;
            }
            else
            {
                ERROR_LOG("Unkown rate!");
                exit(0);
            }

            if (update_num < 1)
            {
                ERROR_LOG("update_num not enought exit!");
                exit(-1);
            }
	    }
    }

#ifdef NIC_MULITI_THREAD
    nic_thread_num = PARTITION_NUMS;
#else
    nic_thread_num = 1;
#endif
    // 初始化 hook
    // set_main_node_thread_addr(&main_node_nic_thread_ptr);
    // set_replica_node_thread_addr(&replica_node_nic_thread_ptr);
    // init_busy_wait_rdma_buff();

    // 初始化本地存储，分配 page
	mehcached_shm_init(page_size, num_numa_nodes, num_pages_to_try, num_pages_to_reserve);

    // 初始化 rdma 连接
    server_instance = dhmp_server_init(SERVER_ID);

    int available_r_node_num;
    if (main_node_is_readable)
        available_r_node_num = server_instance->config.nets_cnt;
    else
        available_r_node_num = (server_instance->config.nets_cnt - 1);

    if (!is_all_set_all_get)
    {
        int __read_num_per_node;
        //int divisible_read_num;
        int left_op_nums;
        int final_get_num;

        read_num = 1000 * read_num;
        update_num = 1000 * update_num;

        __read_num_per_node = read_num / available_r_node_num;  // 丢弃无法整除的部分
        //divisible_read_num = __read_num_per_node * available_r_node_num;

        int i;
        int divisible_get_nums[4];
        //int loop_count_per_round[4];
        memset(divisible_get_nums, 0, sizeof(int)*4);
        // little_idx 之前的 op_gaps[idx] 表示需要每执行一次 set 要执行 for op_gaps[idx] 个 get
        // little_idx 之后(包括little_idx)的 op_gaps[idx] 表示需要每执行 op_gaps[idx] 次 set 要执行 1 次 get
        // 有了 little_idx  之后 get_is_more 就不重要了，只根据 little_idx 判断该进行 for 还是取 mod
        int round=0;
        // 读比写多
        if (__read_num_per_node > update_num)
        {
            ERROR_LOG("Read is more!");
            while(__read_num_per_node > 10 && round < 4)
            {

                if (__read_num_per_node > update_num)
                {
                    op_gaps[round] = __read_num_per_node / update_num;  // 要保留无法整除的部分
                    divisible_get_nums[round] = (op_gaps[round] *update_num)*available_r_node_num;
                    __read_num_per_node = __read_num_per_node - (op_gaps[round]  * update_num); // 要保留无法整除的部分
                }
                else
                {
                    op_gaps[round] = (int)ceil((double)update_num / (double)__read_num_per_node);  // 要保留无法整除的部分
                    divisible_get_nums[round] = (update_num / op_gaps[round])*available_r_node_num;
                    __read_num_per_node = __read_num_per_node - (update_num / op_gaps[round]); // 要保留无法整除的部分
                    if (little_idx==-1)
                        little_idx=round;  
                }
                ERROR_LOG("count:[%d], op_gaps:[%d], divisible_get_nums[%d], __read_num_per_node[%d]\n", round, op_gaps[round], divisible_get_nums[round], __read_num_per_node);
                round++;
                end_round = round;
            }
            // 每执行了 op_gap_2 个 set 之后需要额外执行 1 次 get 
            final_get_num=0;
            for (i=0; i<4; i++)
                    final_get_num += divisible_get_nums[i];

            get_is_more = true;
            ERROR_LOG("FINALLY: update_num[%d], final_get_num:[%d], little_idx[%d], get_is_more[%d]\n",update_num, final_get_num,little_idx, get_is_more);
        }
        // 写比读多
        else
        {
            // op_gaps[0] 表示每隔 op_gaps[0] 个 set 需要执行一次 get
            ERROR_LOG("Write is more!");
            op_gaps[0] = update_num / __read_num_per_node;  // 要保留无法整除的部分
            little_idx = 0;
            end_round = 1;
            left_op_nums = update_num - (op_gaps[0] * __read_num_per_node); // 要保留无法整除的部分
            final_get_num = op_gaps[0] * __read_num_per_node;
            get_is_more = false;
            ERROR_LOG("FINALLY: update_num[%d], __read_num_per_node[%d], left_op_nums:[%d],  final_get_num:[%d], get_is_more[%d]",update_num, __read_num_per_node, left_op_nums, final_get_num ,get_is_more);
        }

        // 更新最终的读数量
        read_num = final_get_num;
        // 更新最终的 access 数量
        __access_num = read_num + update_num;
        Assert(little_idx != -1 && little_idx < end_round);
    }
    else
    {   
        // 纯读纯写的数量默认是 3000
        // 纯写无所谓
        // 纯读需要被副本节点数量整除
        if (read_num % available_r_node_num != 0)
        {
            ERROR_LOG("read_num mod available_r_node_num != 0");
            exit(0);
        }

        // 更新最终的读数量
        // read_num =  read_num / available_r_node_num; 
        // 更新最终的 access 数量
        __access_num = read_num + update_num;
    }
    // op_gap;
    partition_req_count_array = (int*) malloc(sizeof(int) * PARTITION_NUMS);
    memset(partition_req_count_array, 0, sizeof(int) * PARTITION_NUMS);
    generate_local_get_mgs();

    client_mgr = dhmp_client_init(INIT_DHMP_CLIENT_BUFF_SIZE, false);
    Assert(server_instance);
    Assert(client_mgr);
    avg_partition_count_num = update_num /(int) PARTITION_NUMS;


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

    // 主节点和镜像节点初始化本地hash表
    if (IS_MAIN(server_instance->server_type))
    {
        mehcached_table_init(main_table, TABLE_BUCKET_NUMS, 1, TABLE_POOL_SIZE, true, true, true,\
             numa_nodes[0], numa_nodes, MEHCACHED_MTH_THRESHOLD_FIFO);
        mehcached_table_init(log_table, TABLE_BUCKET_NUMS, 1, TABLE_POOL_SIZE, true, true, true,\
             numa_nodes[0], numa_nodes, MEHCACHED_MTH_THRESHOLD_FIFO);

        memset(mirror_node_mapping, 0, sizeof(struct replica_mappings) * PARTITION_MAX_NUMS);
        Assert(main_table);
    }

    if (IS_MIRROR(server_instance->server_type))
    {
        mehcached_table_init(main_table, TABLE_BUCKET_NUMS, 1, TABLE_POOL_SIZE, true, true, true,\
             numa_nodes[0], numa_nodes, MEHCACHED_MTH_THRESHOLD_FIFO);
        Assert(main_table);
    }

    // 主节点初始化远端hash表，镜像节点初始化自己本地的hash表
    //mehcached_node_init();
	if (IS_MAIN(server_instance->server_type))
    {
		MID_LOG("Node [%d] do MAIN node init work", server_instance->server_id);
        // 向所有副本节点发送 main_table 初始化请求
		Assert(server_instance->server_id == MAIN_NODE_ID ||
				server_instance->server_id == MIRROR_NODE_ID);
		size_t nid;
		size_t init_nums = 0;
		bool* inited_state = (bool *) malloc(sizeof(bool) * server_instance->node_nums);
		memset(inited_state, false, sizeof(bool) * server_instance->node_nums);

		// for (nid = REPLICA_NODE_HEAD_ID; nid <= REPLICA_NODE_TAIL_ID; nid++)
		micaserver_get_cliMR(next_node_mappings, REPLICA_NODE_HEAD_ID);

        // 主节点需要知道镜像节点的一块mr地址
        micaserver_get_cliMR(&mirror_node_mapping[0], MIRROR_NODE_ID);

        for (i=1; i<(int)PARTITION_NUMS; i++)
            memcpy(&mirror_node_mapping[i], &mirror_node_mapping[0], sizeof(struct replica_mappings));

		// 等待全部节点初始化完成,才能开放服务
		// 因为是主节点等待，所以主节点主动去轮询，而不是被动等待从节点发送完成信号
		while (init_nums < REPLICA_NODE_NUMS)
		{
			enum ack_info_state ack_state;
            init_nums = 0;
			for (nid = REPLICA_NODE_HEAD_ID; nid <= REPLICA_NODE_TAIL_ID; nid++) 
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

        // 启动网卡线程
        for (i=0; i<nic_thread_num; i++)
        {
            int64_t nic_id = (int64_t)i;
            retval = pthread_create(&nic_thread[i], NULL, main_node_nic_thread, (void*)nic_id);
            if(retval)
            {
                ERROR_LOG("pthread create error.");
                return -1;
            }
        }
		INFO_LOG("---------------------------MAIN node init finished!------------------------------");

#ifdef MAIN_NODE_TEST
        // 主节点启动测试程序
        struct test_kv * kvs = generate_test_data(10, 10, 1024-VALUE_HEADER_LEN-VALUE_TAIL_LEN);
        test_set(kvs);
        struct test_kv * kvs2 = generate_test_data(10, 20, 1024-VALUE_HEADER_LEN-VALUE_TAIL_LEN);
        test_set(kvs2);
        struct test_kv * kvs3 = generate_test_data(10, 30, 1024-VALUE_HEADER_LEN-VALUE_TAIL_LEN);
        test_set(kvs3);
        // test_set(kvs, 100);
        // test_set(kvs, 1000);
#endif
    }

	if (IS_MIRROR(server_instance->server_type))
	{
		// 镜像节点只需要负责初始化自己的hash表即可，不需要知道副本节点的存储地址
		MID_LOG("Node [%d] is mirror node, don't do any init work", server_instance->server_id);
        INFO_LOG("---------------------------MIRROR node init finished!---------------------------");
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
            if (server_instance->node_nums > 3)
            {
                // 向下游节点发送 main_table 初始化请求
                size_t target_id = server_instance->server_id + 1;
                Assert(target_id != server_instance->node_nums);

                micaserver_get_cliMR(next_node_mappings, target_id);
            }

            // 启动网卡线程
            //pthread_create(&nic_thread, NULL, *replica_node_nic_thread_ptr, NULL);
            for (i=0; i<nic_thread_num; i++)
            {
                int64_t nic_id = (int64_t)i;
                retval = pthread_create(&nic_thread[i], NULL, main_node_nic_thread, (void*)nic_id);
                if(retval)
                {
                    ERROR_LOG("pthread create error.");
                    return -1;
                }
            }
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
        
        INFO_LOG("wait mica_ask_nodeID_req");
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

        while(get_table_init_state() == false);

        // 在 replica_is_ready 为真之前副本节点不会接受其他节点发送来的 set, get , update 双边操作
        replica_is_ready = true;
        INFO_LOG("---------------------------Replica Node [%d] init finished!---------------------------", server_instance->server_id);
    }

#ifdef MAIN_LOG_DEBUG_THROUGHOUT
    if (IS_MAIN(server_instance->server_type))
    {
        new_main_test_through();
        exit(0);
    }
#endif

    pthread_join(server_instance->ctx.epoll_thread, NULL);
    // pthread_join(server_instance->ctx.busy_wait_cq_thread, NULL);
    return 0;
}


struct dhmp_msg* 
pack_test_set_resq(struct test_kv * kvs, int tag)
{
    void * base;
    struct dhmp_msg* msg;
	struct post_datagram *req_msg;
	struct dhmp_mica_set_request *req_data;
    size_t key_length  = kvs->true_key_length +  KEY_TAIL_LEN;
    size_t value_length= kvs->true_value_length + VALUE_HEADER_LEN + VALUE_TAIL_LEN;
    size_t total_length = sizeof(struct post_datagram) + sizeof(struct dhmp_mica_set_request) + key_length + value_length;
    size_t tmp_key;
    msg = (struct dhmp_msg*)malloc(sizeof(struct dhmp_msg));
	base = malloc(total_length); 
	memset(base, 0 , total_length);
	req_msg  = (struct post_datagram *) base;
	req_data = (struct dhmp_mica_set_request *)((char *)base + sizeof(struct post_datagram));
	
    // 填充公共报文
	req_msg->node_id = MAIN;	 // 向对端发送自己的 node_id 用于身份辨识
	req_msg->req_ptr = req_msg;
	req_msg->done_flag = false;
	req_msg->info_type = MICA_SET_REQUEST;
	req_msg->info_length = sizeof(struct dhmp_mica_set_request);

	// 填充私有报文
	req_data->current_alloc_id = 0;
	req_data->expire_time = 0;
	req_data->key_hash = kvs->key_hash;
	req_data->key_length = key_length;
	req_data->value_length = value_length;	// 这里的 value 长度是包含了value头部和尾部的长度
	req_data->overwrite = true;
	req_data->is_update = false;
	req_data->tag = (size_t)tag;

    // req_data->partition_id = (int) (*((size_t*)kvs->key)  % (PARTITION_NUMS));
    
    tmp_key = *((size_t*)(kvs->key));
    tmp_key = tmp_key>>16;
	req_data->partition_id = ((int) tmp_key) % ((int)PARTITION_NUMS);

    //ERROR_LOG("tmp_key is %ld, part id:%d",tmp_key, req_data->partition_id);
    Assert(tmp_key <= (size_t)TEST_KV_NUM);
    partition_req_count_array[req_data->partition_id]++;

	memcpy(&(req_data->data), kvs->key, kvs->true_key_length);		// copy key
    memcpy(( (void*)&(req_data->data) + key_length), kvs->value,  kvs->true_value_length);	

    msg->data = base;
    msg->data_size = total_length;
    msg->msg_type = DHMP_MICA_SEND_INFO_REQUEST;
    INIT_LIST_HEAD(&msg->list_anchor);
    msg->trans = NULL;
    msg->recv_partition_id = -1;
    msg->partition_id = req_data->partition_id;
    // msg->main_thread_set_id = 0; main_thread_set_id 在运行时被设置
    Assert(msg->list_anchor.next != LIST_POISON1 && msg->list_anchor.prev!= LIST_POISON2);
    return msg;
}

struct dhmp_msg* 
pack_test_get_resq(struct test_kv * kvs, int tag, size_t expect_length)
{
  	void * base;
	void * data_addr;
    struct dhmp_msg* msg;
	struct post_datagram *req_msg;
	struct dhmp_mica_get_request *req_data;
    struct dhmp_mica_get_response* get_resp;
    size_t key_length = kvs->true_key_length+KEY_TAIL_LEN;
	size_t total_length = sizeof(struct post_datagram) + sizeof(struct dhmp_mica_get_request) + key_length;
    size_t tmp_key;

	// 如果有指针可以 reuse ，那么 reuse
    msg = (struct dhmp_msg*)malloc(sizeof(struct dhmp_msg));
    base = malloc(total_length);
    memset(base, 0 , total_length);
    // get_resp = (struct dhmp_mica_get_response*) malloc(sizeof(struct dhmp_mica_get_response) + expect_length);
    get_resp = NULL;
	req_msg  = (struct post_datagram *) base;
	req_data = (struct dhmp_mica_get_request *)((char *)base + sizeof(struct post_datagram));

	// 填充公共报文
	req_msg->node_id = (int)server_instance->server_id;	 // 向对端发送自己的 node_id 用于身份辨识
	req_msg->req_ptr = req_msg;
	req_msg->done_flag = false;
	req_msg->info_type = MICA_GET_REQUEST;
	req_msg->info_length = sizeof(struct dhmp_mica_get_request);

	// 填充私有报文
	req_data->current_alloc_id = 0;
	req_data->key_hash = kvs->key_hash;
	req_data->key_length = key_length;
	req_data->get_resp = get_resp;
	req_data->peer_max_recv_buff_length = (size_t)expect_length;

    tmp_key = *((size_t*)(kvs->key));
    tmp_key = tmp_key>>16;
	req_data->partition_id = ((int) tmp_key) % ((int)PARTITION_NUMS);

	req_data->tag = (size_t)tag;
	data_addr = (void*)req_data + offsetof(struct dhmp_mica_get_request, data);
	memcpy(data_addr, kvs->key, GET_TRUE_KEY_LEN(key_length));		// copy key

    msg->data = base;
    msg->data_size = total_length;
    msg->msg_type = DHMP_MICA_SEND_INFO_REQUEST;
    INIT_LIST_HEAD(&msg->list_anchor);
    msg->trans = NULL;
    msg->recv_partition_id = -1;
    msg->partition_id = req_data->partition_id;
    Assert(msg->list_anchor.next != LIST_POISON1 && msg->list_anchor.prev!= LIST_POISON2);
    return msg;
}

void workloada_server();

void new_main_test_through()
{
    workloada_server();
}

void generate_local_get_mgs()
{
    // 生成本地读负载
    int i, idx;
    get_msgs_group = (struct dhmp_msg**) malloc( (size_t)(read_num+1 )* sizeof(void*));
    generate_test_data((size_t)0, (size_t)1, (size_t)__test_size , (size_t)TEST_KV_NUM);
    for (i=0; i<=(int)read_num;i++)
    {
        idx = (int)rand_num[i % TEST_KV_NUM];
        get_msgs_group[i] = pack_test_get_resq(&kvs_group[idx], idx, (size_t)__test_size + VALUE_HEADER_LEN + VALUE_TAIL_LEN);
    }
}

// 1：1
void workloada_server()
{
	int i = 0;
    int idx;
    // struct timespec start_t, end_t;
    //struct set_requset_pack req_callback_ptr;	
    // 生成Zipfian数据
    switch (workload_type)
    {
        case UNIFORM:
            pick_uniform(TEST_KV_NUM);
            break;
        case ZIPFIAN:
            pick_zipfian(TEST_KV_NUM);
            break;
        default:
            ERROR_LOG("Unkown!");
            break;
    }
    INFO_LOG("pick");

    struct dhmp_msg** set_msgs_group = (struct dhmp_msg**) malloc( (size_t)update_num * sizeof(void*));
    int * idx_array = (int*) malloc((size_t)ACCESS_NUM * sizeof(int));
    //  生成写负载
    for (i=0; i<(int)update_num;i++)
    {
        idx = (int)rand_num[i % TEST_KV_NUM];
        set_msgs_group[i] = pack_test_set_resq(&kvs_group[idx], idx);
        idx_array[i] = i;
    }

    for (i=0; i<(int)PARTITION_NUMS; i++)
        ERROR_LOG("partition id [%d] counts [%d]", i , partition_req_count_array[i]);
    //exit(0);

    struct timespec start_through, end_through;
    long long int total_set_through_time=0;
    clock_gettime(CLOCK_MONOTONIC, &start_through);
#ifdef PERF_TEST
    int repeat=0;
    while (repeat<1000)
    {
#endif
        for(i=0;i < update_num ;i++)
        {
            bool is_async;
            dhmp_send_request_handler(NULL, set_msgs_group[i], &is_async, 0, 0);
        }
#ifdef PERF_TEST
        repeat++;
    }
#endif
    clock_gettime(CLOCK_MONOTONIC, &end_through);
    total_set_through_time = ((((end_through.tv_sec * 1000000000) + end_through.tv_nsec) - ((start_through.tv_sec * 1000000000) + start_through.tv_nsec)));
    //ERROR_LOG("[set] count[%d] through_out time is [%lld] us", update_num, total_set_through_time /1000);

    // 等待测试结束
    sleep(5);
}

// 测试所有节点中的数据必须一致
void test_get_consistent(struct test_kv * kvs MEHCACHED_UNUSED)
{
    // size_t i, nid;
    // INFO_LOG("---------------------------test_get_consistent!---------------------------");
    // for (i = 0; i < TEST_KV_NUM; i++)
    // {
    //     const uint8_t* key = kvs[i].key;
    //     const uint8_t* value = kvs[i].value;
    //     size_t true_key_length = kvs[i].true_key_length;
    //     size_t true_value_length = kvs[i].true_value_length;
    //     uint64_t key_hash = hash(key, true_key_length);
    //     uint8_t* out_value = (uint8_t*)malloc(true_value_length);
    //     size_t  out_value_length;
    //     uint32_t expire_time;
    //     struct dhmp_mica_get_response *get_result = NULL;

    //     struct mehcached_item * item = kvs[i].item;
    //     Assert(item != NULL);

    //     // 测试本地 table 数据一致
    //     if (!mid_mehcached_get_warpper(0, main_table, key_hash, key, true_key_length,
    //                                  out_value, &out_value_length,
    //                                  &expire_time, false, true))
    //     {
    //         ERROR_LOG("key hash [%lx] get false", key_hash);
    //         Assert(false);
    //     }

    //     if (!cmp_item_value(true_value_length, value, out_value_length, out_value))
    //     {
    //         ERROR_LOG("local item key_hash [%lx] value compare false!", key_hash);
    //         Assert(false);
    //     }
    //     INFO_LOG("No.<%d> Main Node [%d] set test success!", i, MAIN_NODE_ID);

    //     // 测试镜像节点数据一致
    //     get_result = mica_get_remote_warpper(0, key_hash, key, true_key_length, false, NULL, MIRROR_NODE_ID, server_instance->server_id, out_value_length, (size_t)i);
    //     if (get_result == NULL || get_result->out_value_length == (size_t) - 1)
    //     {
    //         ERROR_LOG("MICA get key %lx failed!", key_hash);
    //         Assert(false);
    //     }
    //     if (get_result->partial == true)
    //     {
    //         ERROR_LOG("value too long!");
    //         Assert(false);
    //     }

    //     if (!cmp_item_value(true_value_length, value, MEHCACHED_VALUE_LENGTH(item->kv_length_vec), item_get_value_addr(item)))
    //     {
    //         ERROR_LOG(" item key_hash [%lx] value compare false!", key_hash);
    //         Assert(false);
    //     }

    //     if (!cmp_item_all_value(get_result->out_value_length, get_result->out_value, MEHCACHED_VALUE_LENGTH(item->kv_length_vec), item_get_value_addr(item)))
    //     {
    //         ERROR_LOG("Mirror item key_hash [%lx] value compare false!", key_hash);
    //         Assert(false);
    //     }
    //     free(get_result);
    //     INFO_LOG("No.<%d>, Mirror Node [%d] set test success!", i, MIRROR_NODE_ID);

    //     // 测试远端 replica 节点数据一致
    //     for (nid = REPLICA_NODE_HEAD_ID; nid <= REPLICA_NODE_TAIL_ID; nid++)
    //     {
    //         // 如果 version 不一致，需要反复尝试get直到一致后才会返回结果
    //         while(true)
    //         {
    //             // 远端获取的默认是带header和tailer的value
    //             get_result = mica_get_remote_warpper(0, key_hash, key, true_key_length, false, NULL, nid, server_instance->server_id, out_value_length, (size_t)i);
    //             if (get_result == NULL)
    //             {
    //                 ERROR_LOG("MICA get key %lx failed!", key_hash);
    //                 Assert(false);
    //             }
                
    //             if (get_result->out_value_length != (size_t) - 1)
    //                 break;
    //         }
 
    //         if (get_result->partial == true)
    //         {
    //             ERROR_LOG("value too long!");
    //             Assert(false);
    //         }

    //         if (!cmp_item_value(get_result->out_value_length, get_result->out_value, MEHCACHED_VALUE_LENGTH(item->kv_length_vec), item_get_value_addr(item)))
    //         {
    //             ERROR_LOG("Replica node [%d] item key_hash [%lx] value compare false!", nid, key_hash);
    //             Assert(false);
    //         }
    //         INFO_LOG("No.<%d> Replica Node [%d] set test success!",i, nid);
    //         free(get_result);
    //     }

    //     INFO_LOG("No.<%d> Key_hash [%lx] pas all compare scuess!", i, key_hash);
    // }
    // INFO_LOG("---------------------------test_get_consistent finish!---------------------------");
}


void
test_set(struct test_kv * kvs MEHCACHED_UNUSED)
{
    /*
    INFO_LOG("---------------------------test_set()---------------------------");
    Assert(main_table);

    size_t i;
    size_t nid;
    struct set_requset_pack *req_callback_ptr = (struct set_requset_pack *)\
            malloc(sizeof(struct set_requset_pack) * server_instance->node_nums);
                 
    for (i = 0; i < TEST_KV_NUM; i++)
    {
        bool is_update, is_maintable = true;
        struct mehcached_item * item;
        const uint8_t* key = kvs[i].key;
        //size_t new_value = i + val_offset;
        //memcpy(kvs_group[i].value, &new_value, kvs_group[i].true_value_length);  // 更新value
        const uint8_t* value = kvs[i].value;
        size_t true_key_length = kvs[i].true_key_length;
        size_t true_value_length = kvs[i].true_value_length;
        size_t item_offset;
        uint64_t key_hash = hash(key, true_key_length);
    
        item = midd_mehcached_set_warpper(0, main_table, key_hash,\
                                         key, true_key_length, \
                                         value, true_value_length, 0, true, &is_update, &is_maintable, NULL);

        // Assert(is_update == false);

        if (item == NULL)
        {
            ERROR_LOG("Main node set fail! keyhash is %lx", key_hash);
            exit(0);
        }
        kvs[i].item = item;

        // 镜像节点全value赋值
        for (nid = MIRROR_NODE_ID; nid <= REPLICA_NODE_TAIL_ID; nid++)
        {
            mica_set_remote_warpper(0, 
                                    kvs[i].key,
                                    key_hash, 
                                    true_key_length, 
                                    kvs[i].value,
                                    true_value_length, 
                                    0, true,
                                    true, 
                                    &req_callback_ptr[nid],
                                    nid,
                                    is_update,
                                    server_instance->server_id,
                                    nid);
        }

        for (nid = MIRROR_NODE_ID; nid <= REPLICA_NODE_TAIL_ID; nid++)
        {
            while(req_callback_ptr[nid].req_ptr->done_flag == false);

            if (req_callback_ptr[nid].req_info_ptr->out_mapping_id == (size_t)-1)
            {
                ERROR_LOG("Main node set node[%d] key_hash [%lx] failed!", nid, req_callback_ptr[nid].req_info_ptr->key_hash);
                exit(0);
            }
            else
            {
                // 主节点只需要保存直接下游节点的 mr 信息即可
                if (nid == REPLICA_NODE_HEAD_ID)
                {
                    item->mapping_id = req_callback_ptr[nid].req_info_ptr->out_mapping_id;
                    item->remote_value_addr = req_callback_ptr[nid].req_info_ptr->out_value_addr;
                    INFO_LOG("Main node set node[%d] key_hash [%lx] success!, mapping id is %u, remote addr is %p", \
                                    nid, item->key_hash, item->mapping_id, item->remote_value_addr);
                }
            }

            free(req_callback_ptr[nid].req_ptr);
        }

        INFO_LOG("key hash [%lx] set to all replica node success!", key_hash);

        if (is_maintable)
            item_offset = get_offset_by_item(main_table, item);
        else
            item_offset = get_offset_by_item(log_table, item);
 
        // 只写直接下游节点
        // 还需要返回远端 value 的虚拟地址， 用来求偏移量
        makeup_update_request(item, item_offset,\
                             (uint8_t*)item_get_value_addr(item), \
                             MEHCACHED_VALUE_LENGTH(item->kv_length_vec),
                             nid);
 
        INFO_LOG("key hash [%lx] notices downstream replica node!", key_hash);
    }

    free(req_callback_ptr);
    mehcached_print_stats(main_table);
    mehcached_print_stats(log_table);
    // mehcached_table_free(main_table);
    INFO_LOG("---------------------------test_set finished!---------------------------");
    // 主线程等待1s，让输出更清晰一点
    sleep(1);
    test_get_consistent(kvs);
    */
}