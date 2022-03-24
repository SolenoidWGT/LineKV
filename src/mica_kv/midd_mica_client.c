/*
 * @Author: your name
 * @Date: 2022-03-09 19:58:18
 * @LastEditTime: 2022-03-09 21:23:04
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: /LineKV/src/mica_kv/midd_mica_client.c
 */
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
#include "dhmp_init.h"
#include "mid_rdma_utils.h"
#include "dhmp_top_api.h"

#include "midd_mica_benchmark.h"

bool ica_cli_get(struct test_kv *kv_entry, void *user_buff, size_t *out_value_length, size_t target_id, size_t tag);
struct test_kv * kvs;
 

struct dhmp_client *  
dhmp_test_client_init(size_t buffer_size)
{
	struct dhmp_client *  re_cli;
	re_cli = dhmp_client_init(buffer_size, true);
	re_cli->is_test_clinet = true;
	return re_cli;
}

// 
bool
mica_cli_get(struct test_kv *kv_entry, void *user_buff, size_t *out_value_length, size_t target_id, size_t tag)
{
    bool re;
    struct dhmp_mica_get_response *resp;
    struct dhmp_mica_get_reuse_ptr reuse_ptrs;
    reuse_ptrs.req_base_ptr = NULL;
    reuse_ptrs.resp_ptr = NULL;

Get_Retry:
    mica_get_remote_warpper(0, \
                        kv_entry->key_hash, \
                        kv_entry->key, \
                        kv_entry->true_key_length, \
                        false, \
                        NULL,\
                        target_id, \
                        client_mgr->self_node_id, \
                        *out_value_length, \
                        tag, \
                        &reuse_ptrs);

    resp = reuse_ptrs.resp_ptr;
    if (resp  == NULL)
    {
        ERROR_LOG("MICA get key %lx failed!", kv_entry->key_hash);
        Assert(false);
    }

    INFO_LOG("switch");

    switch (resp->status)
    {
        case MICA_GET_SUCESS:
            INFO_LOG("MICA_GET_SUCESS length is [%ld]", resp->out_value_length);
            *out_value_length = resp->out_value_length;
            // 将数据从 recv_region 中拷贝出去，然后就可以发布 recv 任务了
            memcpy(user_buff, resp->msg_buff_addr, resp->out_value_length);
            INFO_LOG("memcpy");
            re = true;
            INFO_LOG("dhmp_post_recv");
            break;
        case MICA_NO_KEY:
            ERROR_LOG("Tag [%d] not found key from node [%d]!", tag, target_id);
            re = false;
            break;
        case MICA_VERSION_IS_DIRTY:
            ERROR_LOG("Tag [%d] key from node [%d] is dirty, retry!", tag, target_id);
            goto Get_Retry;
        case MICA_GET_PARTIAL:
            ERROR_LOG("Tag [%d] value too long to store in buffer from node [%d]!", tag, target_id);
            re = false;
            break;
        default:
            ERROR_LOG("Tag [%d] Unknow get status Node[%ld]", tag, target_id);
            re = false;
            break;
    }

    // 释放接受缓冲区,此时该块缓冲区就可以被其他的任务可见
    dhmp_post_recv(resp->trans_data.rdma_trans, resp->trans_data.msg->data - sizeof(enum dhmp_msg_type) - sizeof(size_t), PARTITION_NUMS);
    // 释放所有指针
    free(container_of(&(resp->trans_data.msg->data), struct dhmp_msg , data));
    free(reuse_ptrs.req_base_ptr);
    free(reuse_ptrs.resp_ptr);
    INFO_LOG("free");
    return re;
}

// 1：1
void workloada()
{
	int i = 0, j, k;
    size_t idx;
    struct timespec start_t, start_through, end_t, end_through;
    long long int set_time=0, get_time=0, through_time=0;
    int __read_num = read_num;
    int __update_num = update_num;
    int suiji;

    struct set_requset_pack req_callback_ptr[PARTITION_MAX_NUMS];	
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

    size_t out_value_length;
    bool is_update=false;   // is_update 这个参数是不生效的，不会对其进行检查
    clock_gettime(CLOCK_MONOTONIC, &start_through);	
	for(i=0;i < ACCESS_NUM ;)
	{
        int p;
        for (p=0, k=0; k<(int)PARTITION_NUMS && i<ACCESS_NUM; k++, i++, p++)
        {
            j = i % TEST_KV_NUM;
            idx = (size_t)rand_num[j];
            srand((unsigned int)i);
            suiji = rand()%(__read_num+__update_num);
            size_t suiji_node = (size_t) (rand()%(client_mgr->config.nets_cnt - 2));  // 只从副本节点读取
            {
                if(suiji < __read_num)
                {
                    __read_num--;
                    out_value_length = kvs[idx].true_value_length + VALUE_HEADER_LEN + VALUE_TAIL_LEN;
                    INFO_LOG("read from node [%ld], tag is [%ld]", suiji_node, idx);
                    // addr_table_read(addr_table[rand_num[j]],local_buf);
                    clock_gettime(CLOCK_MONOTONIC, &start_t);	
                    mica_cli_get(&kvs[idx], \
                                kvs[idx].get_value[0], \
                                &out_value_length, \
                                suiji_node, \
                                idx);
                    clock_gettime(CLOCK_MONOTONIC, &end_t);	
                    get_time += (((end_t.tv_sec * 1000000000) + end_t.tv_nsec) - ((start_t.tv_sec * 1000000000) + start_t.tv_nsec)); 
                }
                else
                {
                    __update_num --;
                    // addr_table_update(addr_table[rand_num[j]],local_buf);
                    INFO_LOG("set tag is [%ld], left update nums [%d]", idx, __update_num);
                    clock_gettime(CLOCK_MONOTONIC, &start_t);	
                    mica_set_remote_warpper(0, 
                                            kvs[idx].key,
                                            kvs[idx].key_hash, 
                                            kvs[idx].true_key_length, 
                                            kvs[idx].value,
                                            kvs[idx].true_value_length, 
                                            0, true,
                                            true, 
                                            &req_callback_ptr[k],
                                            MAIN,
                                            is_update,
                                            client_mgr->self_node_id,
                                            (size_t)i);
                    clock_gettime(CLOCK_MONOTONIC, &end_t);	
                    set_time += (((end_t.tv_sec * 1000000000) + end_t.tv_nsec) - ((start_t.tv_sec * 1000000000) + start_t.tv_nsec)); 
                }
            }

            if (i % 1000 == 0)
                ERROR_LOG("Count [%d]", i);
        }
    
        for (k=0; k<p; k++)
        {
            while(!req_callback_ptr[k].req_ptr->done_flag);
        }
	}
    clock_gettime(CLOCK_MONOTONIC, &end_through);	
    through_time = (((end_through.tv_sec * 1000000000) + end_through.tv_nsec) - ((start_through.tv_sec * 1000000000) + start_through.tv_nsec)); 
    if (read_num!=0)
        ERROR_LOG("workloada test FINISH! avg get time is [%ld]us",  get_time /( US_BASE * read_num));
    if (update_num!=0)
        ERROR_LOG("workloada test FINISH! avg set time is [%ld]us",  set_time /( US_BASE * update_num));

    ERROR_LOG("set count [%d] get count [%d] total use [%ld] us ", read_num, update_num, through_time/1000);
}

void debug_test()
{
    int i, j, reval;
    long long int total_time=0, total_set_time;
    struct timespec start_t, end_t;
    Assert(client_mgr);

    for (i = 0; i < TEST_KV_NUM; i++)
    {
        bool is_update = false;
        clock_gettime(CLOCK_MONOTONIC, &start_t);	
        mica_set_remote_warpper(0, 
                                kvs[i].key,
                                kvs[i].key_hash, 
                                kvs[i].true_key_length, 
                                kvs[i].value,
                                kvs[i].true_value_length, 
                                0, true,
                                false, 
                                NULL,
                                MAIN,
                                is_update,
                                client_mgr->self_node_id,
                                (size_t)i);

    	clock_gettime(CLOCK_MONOTONIC, &end_t);			    	
		total_time += (((end_t.tv_sec * 1000000000) + end_t.tv_nsec) - ((start_t.tv_sec * 1000000000) + start_t.tv_nsec)); 
        // ERROR_LOG("Mica test clinet set tag [%d] key success", i);
    }

    INFO_LOG("Mica clinet set all keys!");
    total_set_time = total_time;
    total_time=0;

    for (j=0; j<client_mgr->config.nets_cnt; j++)
    {
        INFO_LOG("Mica clinet get key form node [%d]!", j);
        for (i = 0; i < TEST_KV_NUM; i++)
        {
            // 和main节点的数据进行比较
            uint8_t *value_addr = NULL;
            size_t out_value_length = kvs[i].true_value_length + VALUE_HEADER_LEN + VALUE_TAIL_LEN;
            // 使用远端接口get回来的value包含了元数据，需要去掉元数据获得真实数据

            clock_gettime(CLOCK_MONOTONIC, &start_t);	
            reval = mica_cli_get(&kvs[i], \
                                  kvs[i].get_value[0], \
                                  &out_value_length, \
                                  (size_t)j, \
                                  (size_t)i);

            if (reval == false)
            {
                ERROR_LOG("Mica clinet get key tag [%d] form node [%d] ERROR!", i, j);
                Assert(false);
            }
            clock_gettime(CLOCK_MONOTONIC, &end_t);	
            total_time += (((end_t.tv_sec * 1000000000) + end_t.tv_nsec) - ((start_t.tv_sec * 1000000000) + start_t.tv_nsec)); 

            INFO_LOG("mica_cli_get");
            value_addr = kvs[i].get_value[0];
            value_addr = value_addr + VALUE_HEADER_LEN;
            out_value_length = out_value_length - VALUE_HEADER_LEN - VALUE_TAIL_LEN;
            // if (!cmp_item_all_value(get_result->out_value_length, get_result->out_value, MEHCACHED_VALUE_LENGTH(item->kv_length_vec), item_get_value_addr(item)))

            if (!cmp_item_all_value(out_value_length, value_addr, kvs[i].true_value_length, kvs[i].value))
            {
                ERROR_LOG("Key id [%d] value is not consistent with main node!", i);
                Assert(false);
            }
        }

        ERROR_LOG("avg set time: [%lld]us", total_set_time /( US_BASE * TEST_KV_NUM));
        ERROR_LOG("avg get time: [%lld]us", total_time / (US_BASE * TEST_KV_NUM * client_mgr->config.nets_cnt));
        ERROR_LOG("MICA_MIDD_TEST_client from node [%d] FINISH!", j);
    }
}

int main(int argc,char *argv[])
{
    Assert(TEST_KV_NUM < TABLE_BUCKET_NUMS);
    int i, reval;
    INFO_LOG("Server argc is [%d]", argc);
    Assert(argc==7);
    for (i = 0; i<argc; i++)
	{
        if (i==1)
        {
            CLINET_ID = (size_t)(*argv[i] - '0');
            INFO_LOG(" node_id is [%d]", CLINET_ID);
        }
        else if (i==2)
        {
            __partition_nums = (unsigned long long)atoi(argv[i]);
            Assert(__partition_nums >0 && __partition_nums < PARTITION_MAX_NUMS);
            INFO_LOG(" __partition_nums is [%d]", __partition_nums);
        }
        else if (i==3)
        {
            __test_size = atoi(argv[i]);
            INFO_LOG(" __test_size is [%d]", __test_size);
        }
        else if (i==4)
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
        else if (i==5)
        {
            __access_num = atoi(argv[i]);
            INFO_LOG(" __access_num is [%d]", __access_num);
        }
        else if (i==6)
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

    INFO_LOG("MICA_MIDD_TEST_client");
    client_mgr = dhmp_client_init(INIT_DHMP_CLIENT_BUFF_SIZE, true);
    client_mgr->self_node_id = CLINET_ID;   // 客户端不需要在 config 文件中出现，但是仍然需要显式指定一个 Node ID

    for (i=0; i<client_mgr->config.nets_cnt; i++)
    {
        INFO_LOG("CONNECT BEGIN: create the [%d]-th normal transport.",i);
        reval = mica_clinet_connect_server(INIT_DHMP_CLIENT_BUFF_SIZE, i);
        if (reval == -1)
            Assert(false);
    }

    // kvs = generate_test_data(1, 1, 1024-VALUE_HEADER_LEN-VALUE_TAIL_LEN, TEST_KV_NUM, (size_t)client_mgr->config.nets_cnt);
    kvs = generate_test_data(1, 1, (size_t)__test_size , TEST_KV_NUM);

    workloada();
    sleep(100);
    return 0;
}