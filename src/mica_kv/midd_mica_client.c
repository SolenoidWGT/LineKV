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

#define TEST_KV_NUMS  64
// #define TEST_KV_NUMS 2

volatile bool replica_is_ready = true;
bool ica_cli_get(struct test_kv *kv_entry, void *user_buff, size_t *out_value_length, size_t target_id, size_t tag);

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
            // 释放接受缓冲区,此时该块缓冲区就可以被其他的任务可见
            dhmp_post_recv(resp->trans_data.rdma_trans, resp->trans_data.msg->data - sizeof(enum dhmp_msg_type) - sizeof(size_t));
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

    // 释放所有指针
    free(container_of(&(resp->trans_data.msg->data), struct dhmp_msg , data));
    free(reuse_ptrs.req_base_ptr);
    free(reuse_ptrs.resp_ptr);
    INFO_LOG("free");
    return re;
}

int main(int argc,char *argv[])
{
    int i, j, reval;
    struct test_kv * kvs;
    struct timespec start_t, end_t;
    long long int total_time=0, total_set_time;

    Assert(TEST_KV_NUMS < TABLE_BUCKET_NUMS);

    for (i = 0; i<argc; i++)
	{
		CLINET_ID = (size_t)(*argv[i] - '0');
        INFO_LOG("Client is %d", CLINET_ID);
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

    kvs = generate_test_data(1, 1, 1024-VALUE_HEADER_LEN-VALUE_TAIL_LEN, TEST_KV_NUMS, client_mgr->config.nets_cnt);
    Assert(client_mgr);

    for (i = 0; i < TEST_KV_NUMS; i++)
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
        for (i = 0; i < TEST_KV_NUMS; i++)
        {
            // 和main节点的数据进行比较
            uint8_t *value_addr = NULL;
            size_t out_value_length = kvs[i].true_value_length + VALUE_HEADER_LEN + VALUE_TAIL_LEN;
            // 使用远端接口get回来的value包含了元数据，需要去掉元数据获得真实数据

            clock_gettime(CLOCK_MONOTONIC, &start_t);	
            reval = mica_cli_get(&kvs[i], kvs[i].get_value[j],  &out_value_length, (size_t)j, (size_t)i);
            if (reval == false)
            {
                ERROR_LOG("Mica clinet get key tag [%d] form node [%d] ERROR!", i, j);
                Assert(false);
            }
            clock_gettime(CLOCK_MONOTONIC, &end_t);	
            total_time += (((end_t.tv_sec * 1000000000) + end_t.tv_nsec) - ((start_t.tv_sec * 1000000000) + start_t.tv_nsec)); 

            INFO_LOG("mica_cli_get");
            value_addr = kvs[i].get_value[j];
            value_addr = value_addr + VALUE_HEADER_LEN;
            out_value_length = out_value_length - VALUE_HEADER_LEN - VALUE_TAIL_LEN;
            // if (!cmp_item_all_value(get_result->out_value_length, get_result->out_value, MEHCACHED_VALUE_LENGTH(item->kv_length_vec), item_get_value_addr(item)))

            if (!cmp_item_all_value(out_value_length, value_addr, kvs[i].true_value_length, kvs[i].value))
            {
                ERROR_LOG("Key id [%d] value is not consistent with main node!", i);
                Assert(false);
            }
        }

        ERROR_LOG("avg set time: [%lld]us", total_set_time /( US_BASE * TEST_KV_NUMS));
        ERROR_LOG("avg get time: [%lld]us", total_time / (US_BASE * TEST_KV_NUMS * client_mgr->config.nets_cnt));
        ERROR_LOG("MICA_MIDD_TEST_client from node [%d] FINISH!", j);
    }
    return 0;
}