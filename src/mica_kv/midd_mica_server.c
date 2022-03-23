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
static size_t SERVER_ID= (size_t)-1;
struct test_kv kvs_group[TEST_KV_NUM];
static void free_test_date();

int main(int argc,char *argv[])
{
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
            if(strcmp(argv[i], "1:1") == 0)
            {
                INFO_LOG(" RW_TATE is [%s]", argv[i]);
                read_num = ACCESS_NUM /2;
                update_num = ACCESS_NUM /2;
            }
            else if(strcmp(argv[i], "0:1") == 0)
            {
                INFO_LOG(" RW_TATE is [%s]", argv[i]);
                read_num = 0;
                update_num = ACCESS_NUM;
            }
            else if(strcmp(argv[i], "1:0") == 0)
            {
                INFO_LOG(" RW_TATE is [%s]", argv[i]);
                read_num = ACCESS_NUM;
                update_num = 0;
            }
            else if(strcmp(argv[i], "4:1") == 0)
            {
                INFO_LOG(" RW_TATE is [%s]", argv[i]);
                read_num = (int)(0.8 * ACCESS_NUM);
                update_num = (int)(0.2 * ACCESS_NUM);
            }
            else
            {
                ERROR_LOG("Unkown rate!");
                exit(0);
            }
        }
	}
#ifdef NIC_MULITI_THREAD
    nic_thread_num = PARTITION_NUMS;
#else
    nic_thread_num = 1;
#endif

    // 初始化本地存储，分配 page
	mehcached_shm_init(page_size, num_numa_nodes, num_pages_to_try, num_pages_to_reserve);
    mehcached_table_init(main_table, TABLE_BUCKET_NUMS, 1, TABLE_POOL_SIZE, true, true, true, numa_nodes[0], numa_nodes, MEHCACHED_MTH_THRESHOLD_FIFO);
    set_table_init_state(true);
    replica_is_ready = true;

    // 初始化 rdma 连接
    server_instance = dhmp_server_init(SERVER_ID);
    client_mgr = dhmp_client_init(INIT_DHMP_CLIENT_BUFF_SIZE, false);
    Assert(server_instance);
    Assert(client_mgr);

    next_node_mappings = (struct replica_mappings *) malloc(sizeof(struct replica_mappings));
    memset(next_node_mappings, 0, sizeof(struct replica_mappings));
    // 将mica的所有并发程度都调成最大
    INFO_LOG("---------------------------CRAQ Node [%d] init finished!---------------------------", server_instance->server_id);

    // if (IS_HEAD(server_instance->server_type))
    // {
    //     new_main_test_through();
    //     sleep(10);
    // }

    pthread_join(server_instance->ctx.epoll_thread, NULL);
    return 0;
}

struct dhmp_msg* 
pack_test_resq(struct test_kv * kvs, int tag)
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
	req_data->current_alloc_id = 0;
	req_data->expire_time = 0;
	req_data->key_hash = kvs->key_hash;
	req_data->key_length = key_length;
	req_data->value_length = value_length;	// 这里的 value 长度是包含了value头部和尾部的长度
	req_data->overwrite = true;
	req_data->is_update = false;
	req_data->tag = (size_t)tag;
    req_data->partition_id = (int) (*((size_t*)kvs->key)  % (PARTITION_NUMS));
	memcpy(&(req_data->data), kvs->key, kvs->true_key_length);		// copy key
    memcpy(( (void*)&(req_data->data) + key_length), kvs->value,  kvs->true_value_length);	

    msg->data = base;
    msg->data_size = total_length;
    msg->msg_type = DHMP_MICA_SEND_INFO_REQUEST;
    INIT_LIST_HEAD(&msg->list_anchor);
    msg->trans = NULL;
    msg->recv_partition_id = -1;
    Assert(msg->list_anchor.next != LIST_POISON1 && msg->list_anchor.prev!= LIST_POISON2);
    return msg;
}

void workloada_server(struct test_kv * kvs_group);

void new_main_test_through()
{
    struct test_kv * kvs_group = generate_test_data((size_t)0, (size_t)1, (size_t)__test_size , (size_t)TEST_KV_NUM);
    workloada_server(kvs_group);
}

// 1：1
void workloada_server(struct test_kv * kvs_group)
{
	int i = 0;
    int idx;
    // struct timespec start_t, end_t;
    long long int set_time=0, get_time=0;
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

    struct dhmp_msg** set_msg_pack = (struct dhmp_msg**) malloc( (size_t)ACCESS_NUM * sizeof(void*));
    int * idx_array = (int*) malloc((size_t)ACCESS_NUM * sizeof(int));
    for (i=0; i<(int)ACCESS_NUM;i++)
    {
        idx = (int)rand_num[i % TEST_KV_NUM];
        set_msg_pack[i] = pack_test_resq(&kvs_group[idx], idx);
        idx_array[i] = i;
    }

    struct timespec start_through, end_through;
    long long int total_set_through_time=0;
	for(i=0;i < ACCESS_NUM ;i++)
	{
        bool is_async;
        if (set_counts == 100)
            clock_gettime(CLOCK_MONOTONIC, &start_through);

        dhmp_send_request_handler(NULL, set_msg_pack[i], &is_async);

        set_counts++;
        if (set_counts == (uint64_t)__access_num) 
        {
            clock_gettime(CLOCK_MONOTONIC, &end_through);
            total_set_through_time = ((((end_through.tv_sec * 1000000000) + end_through.tv_nsec) - ((start_through.tv_sec * 1000000000) + start_through.tv_nsec)));
            ERROR_LOG("[set] count[%d] through_out time is [%lld] us", (__access_num-100), total_set_through_time /1000);
            for (i=0; i<(int)PARTITION_NUMS; i++)
                ERROR_LOG("partition[%d] set count [%d]",i, partition_set_count[i]);
        }
	}

    if (read_num!=0)
        ERROR_LOG("workloada test FINISH! avg get time is [%ld]us",  get_time /( US_BASE * read_num));
    if (update_num!=0)
        ERROR_LOG("workloada test FINISH! avg set time is [%ld]us",  set_time /( US_BASE * update_num));
    
    sleep(10);
}


// 测试所有节点中的数据必须一致
void test_get_consistent(struct test_kv * kvs MEHCACHED_UNUSED)
{

}

void
test_set(struct test_kv * kvs MEHCACHED_UNUSED)
{
}
