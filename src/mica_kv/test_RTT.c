/*
 * @Author: your name
 * @Date: 2022-03-20 21:47:18
 * @LastEditTime: 2022-03-20 23:36:06
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: /LineKV/src/mica_kv/test_RTT.c
 */
#include "mehcached.h"
#include "dhmp_client.h"
#include "dhmp_server.h"
#include "dhmp.h"
#include "dhmp_log.h"
#include "dhmp_top_api.h"
#include "dhmp_init.h"


// struct dhmp_server *server_instance=NULL;
// struct dhmp_client *client_mgr=NULL;

#define SERVER_NODE_ID 0
#define CLIENT_NODE_ID 1

int main(int argc,char *argv[])
{
    int i, is_server, reval;
    __partition_nums = 6;
    for (i = 1; i<argc; i++)
        is_server = atoi(argv[i]);

    if (is_server)
    {
        INFO_LOG("server");
        server_instance = dhmp_server_init(0);
        pthread_join(server_instance->ctx.epoll_thread, NULL);
    }
    else
    {
        INFO_LOG("-------client--------");

        struct timespec start, end;
        long long total_set_time =0;
        int time_count_array[20];
        memset(time_count_array, 0, sizeof(int) * 20);
        client_mgr = dhmp_client_init(INIT_DHMP_CLIENT_BUFF_SIZE, true);
        init_mulit_server_work_thread();
        reval = mica_clinet_connect_server(INIT_DHMP_CLIENT_BUFF_SIZE, SERVER_NODE_ID);
        struct dhmp_msg req_msg;
        if (reval == -1)
            Assert(false);

        for (i=0; i<1000;i++)
        {
            struct post_datagram req;
            long long this_time;
            req.req_ptr  = &req;		    		
            req.resp_ptr = NULL;							
            req.node_id  = CLIENT_NODE_ID;
            req.info_type = MICA_SET_REQUEST_TEST;
            req.info_length = 0;
            req.done_flag = false;						

            req_msg.msg_type = DHMP_MICA_SEND_INFO_REQUEST;
            req_msg.data_size = sizeof(struct post_datagram);
            req_msg.data= &req;
            req_msg.recv_partition_id = -1;
            INIT_LIST_HEAD(&req_msg.list_anchor);
            dhmp_post_send(find_connect_server_by_nodeID(SERVER_NODE_ID), &req_msg, PARTITION_NUMS);

            clock_gettime(CLOCK_MONOTONIC, &start);
            while(req.done_flag == false);
            clock_gettime(CLOCK_MONOTONIC, &end);
            this_time = (((end.tv_sec * 1000000000) + end.tv_nsec) - ((start.tv_sec * 1000000000) + start.tv_nsec));
            total_set_time += this_time;
            time_count_array[this_time/1000]++;
            // ERROR_LOG("time is [%lld]us", total_set_time/1000);
        }

        ERROR_LOG("avg time is [%ld]", total_set_time/(1000*1000));

        for (i=0; i<20; i++)
            ERROR_LOG("[%d]us count [%d]", i, time_count_array[i]);
    }
    return 0;
}