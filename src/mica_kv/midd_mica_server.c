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

#include "mica_partition.h"
#include "midd_mica_benchmark.h"
void new_main_test_through();

pthread_t nic_thread[PARTITION_MAX_NUMS];

void* (*main_node_nic_thread_ptr) (void* );
void* (*replica_node_nic_thread_ptr) (void* );
void test_set(struct test_kv * kvs);

struct dhmp_msg* all_access_set_group;
static size_t SERVER_ID= (size_t)-1;
bool is_single_thread;
int test_size;

void generate_local_get_mgs();

int main(int argc,char *argv[])
{
    INFO_LOG("----------------------------------------CHT----------------------------------------");
    // 初始化集群 rdma 连接
    int i, retval MEHCACHED_UNUSED;
    int nic_thread_num MEHCACHED_UNUSED;
    size_t numa_nodes[] = {(size_t)-1};;
    const size_t page_size = 4*1024UL;
	const size_t num_numa_nodes = 2;
    const size_t num_pages_to_try = 16384;
    const size_t num_pages_to_reserve = 16384;
    
    INFO_LOG("Server argc is [%d]", argc);
    INFO_LOG("Server argc is [%d]", argc);
    Assert(argc==8);
    main_node_is_readable = false;
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

    // 初始化本地存储，分配 page
	mehcached_shm_init(page_size, num_numa_nodes, num_pages_to_try, num_pages_to_reserve);
    mehcached_table_init(main_table, TABLE_BUCKET_NUMS, 1, TABLE_POOL_SIZE, true, true, true, numa_nodes[0], numa_nodes, MEHCACHED_MTH_THRESHOLD_FIFO);
   
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
    generate_local_get_mgs();

    client_mgr = dhmp_client_init(INIT_DHMP_CLIENT_BUFF_SIZE, false);
    Assert(server_instance);
    Assert(client_mgr);
    Assert(!IS_REPLICA(server_instance->server_type));

    next_node_mappings = (struct replica_mappings *) malloc(sizeof(struct replica_mappings));
    memset(next_node_mappings, 0, sizeof(struct replica_mappings));

    // 主节点和镜像节点初始化本地hash表
    if (IS_MAIN(server_instance->server_type))
    {
        Assert(server_instance->server_id == 0);
    	INFO_LOG("---------------------------MAIN node init finished!------------------------------");
        new_main_test_through();
        Assert(main_table);
    }

    if (IS_MIRROR(server_instance->server_type))
    {
        Assert(main_table);
		MID_LOG("Node [%d] is mirror node, don't do any init work", server_instance->server_id);
        INFO_LOG("---------------------------MIRROR node init finished!---------------------------");
    }

    pthread_join(server_instance->ctx.epoll_thread, NULL);
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
    req_data->onePC = true;
	req_data->current_alloc_id = 0;
	req_data->expire_time = 0;
	req_data->key_hash = kvs->key_hash;
	req_data->key_length = key_length;
	req_data->value_length = value_length;	// 这里的 value 长度是包含了value头部和尾部的长度
	req_data->overwrite = true;
	req_data->is_update = false;
	req_data->tag = (size_t)tag;
    //req_data->partition_id = (int) (*((size_t*)kvs->key)  % (PARTITION_NUMS));
    size_t tmp_key = *(size_t*)(kvs->key);
	req_data->partition_id = (int) ( (int)(tmp_key>>16)  % (int)(PARTITION_NUMS));

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
	//req_data->partition_id = (int) (*((size_t*)kvs->key)  % (PARTITION_NUMS));
    size_t tmp_key = *(size_t*)(kvs->key);
	req_data->partition_id = (int) ( (int)(tmp_key>>16)  % (int)(PARTITION_NUMS));

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

    struct timespec start_through, end_through;
    long long int total_set_through_time=0;
    clock_gettime(CLOCK_MONOTONIC, &start_through);
	for(i=0;i < update_num ;i++)
	{
        bool is_async;
        dhmp_send_request_handler(NULL, set_msgs_group[i], &is_async);
	}
    clock_gettime(CLOCK_MONOTONIC, &end_through);
    total_set_through_time = ((((end_through.tv_sec * 1000000000) + end_through.tv_nsec) - ((start_through.tv_sec * 1000000000) + start_through.tv_nsec)));
    ERROR_LOG("[set] count[%d] through_out time is [%lld] us", update_num, total_set_through_time /1000);

    sleep(10);
}

void
test_set(struct test_kv * kvs MEHCACHED_UNUSED)
{}



// 测试所有节点中的数据必须一致
void test_get_consistent(struct test_kv * kvs MEHCACHED_UNUSED)
{}