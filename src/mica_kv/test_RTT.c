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
    int i, is_server=1, reval;
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
        int time_count_array[50];
        memset(time_count_array, 0, sizeof(int) * 50);
        client_mgr = dhmp_client_init(INIT_DHMP_CLIENT_BUFF_SIZE, true);
        init_mulit_server_work_thread();
        reval = mica_clinet_connect_server(INIT_DHMP_CLIENT_BUFF_SIZE, SERVER_NODE_ID);
        struct dhmp_msg req_msg;
        if (reval == -1)
            Assert(false);
        
        micaserver_get_cliMR(&mirror_node_mapping[0], SERVER_NODE_ID);

        ERROR_LOG("INIT finish!");
        for (i=0; i<1000;i++)
        {
            void *base;
            size_t total_length = sizeof(struct post_datagram) + (size_t)64000;
            base = malloc(total_length);
            struct post_datagram *req = (struct post_datagram *)base;
            long long this_time;
            req->req_ptr  = req;		    		
            req->resp_ptr = NULL;							
            req->node_id  = CLIENT_NODE_ID;
            req->info_type = MICA_SET_REQUEST_TEST;
            req->info_length = 0;
            req->done_flag = false;						

            req_msg.msg_type = DHMP_MICA_SEND_INFO_REQUEST;
            req_msg.data_size = total_length;
            req_msg.data= base;
            req_msg.recv_partition_id = -1;
            INIT_LIST_HEAD(&req_msg.list_anchor);

            clock_gettime(CLOCK_MONOTONIC, &start);
            dhmp_post_send(find_connect_server_by_nodeID(SERVER_NODE_ID), &req_msg, 0);
            while(req->done_flag == false);
            clock_gettime(CLOCK_MONOTONIC, &end);
            this_time = (((end.tv_sec * 1000000000) + end.tv_nsec) - ((start.tv_sec * 1000000000) + start.tv_nsec));
            total_set_time += this_time;
            time_count_array[this_time/1000]++;
            // ERROR_LOG("time is [%lld]us", total_set_time/1000);
        }
        ERROR_LOG("post send avg time is [%ld]", total_set_time/(1000*1000));
        for (i=0; i<50; i++)
            ERROR_LOG("post send  [%d]us count [%d]", i, time_count_array[i]);
        ERROR_LOG("---------------------------------------------");

        memset(time_count_array, 0, sizeof(int) * 50);
        total_set_time=0;
        for (i=0; i<1000;i++)
        {
            long long this_time;
            struct dhmp_task write_task;
			struct ibv_sge sge;
			struct ibv_send_wr send_wr,*bad_wr=NULL;
            struct dhmp_transport* rdma_trans = find_connect_server_by_nodeID(SERVER_NODE_ID);

			write_task.done_flag = false;
			write_task.is_imm = true;
			mirror_node_mapping[0].in_used_flag = 1;

			memset(&send_wr, 0, sizeof(struct ibv_send_wr));
			send_wr.opcode=IBV_WR_RDMA_WRITE_WITH_IMM;
			send_wr.imm_data  = htonl((uint32_t)0);
			send_wr.wr_id= ( uintptr_t ) &write_task;
			send_wr.sg_list=&sge;
			send_wr.num_sge=1;
			send_wr.send_flags=IBV_SEND_SIGNALED;
			send_wr.wr.rdma.remote_addr = (uintptr_t)(mirror_node_mapping[0].mirror_virtual_addr);  // WGT
			send_wr.wr.rdma.rkey  		= mirror_node_mapping[0].mirror_mr.rkey;
			sge.addr  =	(uintptr_t)(rdma_trans->send_mr[0].addr);
			sge.length=	65536;
			sge.lkey  =	rdma_trans->send_mr[0].mr->lkey;

            clock_gettime(CLOCK_MONOTONIC, &start);
			reval=ibv_post_send ( rdma_trans->qp, &send_wr, &bad_wr );
			if ( reval )
			{
				ERROR_LOG("ibv_post_send error[%d], reason is [%s]", errno,strerror(errno));
				exit(-1);
			}
            while(mirror_node_mapping[0].in_used_flag == 1);
            clock_gettime(CLOCK_MONOTONIC, &end);
            this_time = (((end.tv_sec * 1000000000) + end.tv_nsec) - ((start.tv_sec * 1000000000) + start.tv_nsec));
            total_set_time += this_time;
            time_count_array[this_time/1000]++;
        }

        ERROR_LOG("imm avg time is [%ld]", total_set_time/(1000*1000));
        for (i=0; i<50; i++)
            ERROR_LOG("imm [%d]us count [%d]", i, time_count_array[i]);
        ERROR_LOG("---------------------------------------------");
    }
    return 0;
}