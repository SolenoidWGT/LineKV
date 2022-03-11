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

#define TEST_KV_NUMS  8

volatile bool replica_is_ready = true;

struct dhmp_client *  
dhmp_test_client_init(size_t buffer_size)
{
	struct dhmp_client *  re_cli;
	re_cli = dhmp_client_init(buffer_size, true);
	re_cli->is_test_clinet = true;
	return re_cli;
}

uint8_t* mica_cli_get(struct test_kv *kv_entry, size_t *out_value_length)
{
    struct dhmp_mica_get_response *get_result =\
             mica_get_remote_warpper(0, kv_entry->key_hash, kv_entry->key, kv_entry->true_key_length, false, NULL, MAIN, client_mgr->self_node_id);
    
    if (get_result == NULL || get_result->out_value_length == (size_t) - 1)
    {
        ERROR_LOG("MICA get key %lx failed!", kv_entry->key_hash);
        Assert(false);
    }
    if (get_result->partial == true)
    {
        ERROR_LOG("value too long!");
        Assert(false);
    }
    *out_value_length = get_result->out_value_length;
    return get_result->out_value;
}

int main(int argc,char *argv[])
{
    int i;
    struct test_kv * kvs;

    for (i = 0; i<argc; i++)
	{
		CLINET_ID = (size_t)(*argv[i] - '0');
        INFO_LOG("Client is %d", CLINET_ID);
	}

    INFO_LOG("MICA_MIDD_TEST_client");
    client_mgr = dhmp_client_init(INIT_DHMP_CLIENT_BUFF_SIZE, true);
    kvs = generate_test_data(1, 1, 1024-VALUE_HEADER_LEN-VALUE_TAIL_LEN, TEST_KV_NUMS);
    Assert(client_mgr);

    for (i = 0; i < TEST_KV_NUMS; i++)
    {
        bool is_update = false;

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
                                client_mgr->self_node_id);
        
        INFO_LOG("Mica test clinet set [%d] key success");
    }

    for (i = 0; i < TEST_KV_NUMS; i++)
    {
        // 和main节点的数据进行比较
        size_t out_value_length;
        // 使用远端接口get回来的value包含了元数据，需要去掉元数据获得真实数据
        uint8_t * value = mica_cli_get(&kvs[i], &out_value_length);
        value = value + VALUE_HEADER_LEN;
        out_value_length = out_value_length - VALUE_HEADER_LEN - VALUE_TAIL_LEN;
        // if (!cmp_item_all_value(get_result->out_value_length, get_result->out_value, MEHCACHED_VALUE_LENGTH(item->kv_length_vec), item_get_value_addr(item)))

        if (!cmp_item_all_value(out_value_length, value, kvs[i].true_value_length, kvs[i].value))
        {
            ERROR_LOG("Key id [%d] value is not consistent with main node!", i);
            Assert(false);
        }

        INFO_LOG("Mica test clinet set [%d] compare success");
    }
    INFO_LOG("MICA_MIDD_TEST_client FINISH!");
    return 0;
}