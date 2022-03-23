#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <arpa/inet.h>

#include "dhmp.h"
#include "dhmp_transport.h"
#include "dhmp_task.h"
#include "dhmp_client.h"
#include "dhmp_log.h"
#include "dhmp_dev.h"
#include "dhmp_server.h"

/**
 *	the success work completion handler function
 * 
 *  dhmp_post_recv 在多线程卸载之后必须要放置到卸载的工作线程中去执行，主线程不能执行 dhmp_post_recv
 *  否则无法保证接收缓冲区已经可以被修改。
 */
static void dhmp_wc_success_handler(struct ibv_wc* wc)
{
	struct dhmp_task *task_ptr;
	struct dhmp_transport *rdma_trans;
	struct dhmp_msg *msg;
	struct post_datagram *req_datagram;
	// 由于我们异步化了 wc 处理，所以必须把 msg 变成堆上内存而不是栈中内存。
	// struct dhmp_msg msg;
	
	bool is_async = false;
	size_t peer_partition_id = (size_t)-1;
	int recv_partition_id;
	
	//DEFINE_STACK_TIMER();
	struct timespec start, end;
	task_ptr=(struct dhmp_task*)(uintptr_t)wc->wr_id;
	rdma_trans=task_ptr->rdma_trans;
	recv_partition_id= task_ptr->partition_id;

	switch(wc->opcode)
	{
		case IBV_WC_SEND:
			break;

		case IBV_WC_RECV_RDMA_WITH_IMM: // 镜像节点(imm的接收方)唤醒，去处理主节点的写请求
			// 由于发送方没有提供 msg， 所以需要自己 malloc 一个
			Assert(IS_MIRROR(server_instance->server_type));
			msg = (struct dhmp_msg *) malloc(sizeof(struct dhmp_msg) + sizeof(struct post_datagram));
			req_datagram = (struct post_datagram *)((char*)msg + sizeof(struct dhmp_msg));
			msg->msg_type = DHMP_MICA_SEND_INFO_REQUEST;
			msg->data_size = 0;
			msg->data = req_datagram;
			// 以下数据不会从报文中获取
			INIT_LIST_HEAD(&msg->list_anchor);
			msg->trans = rdma_trans;
			peer_partition_id = (size_t)ntohl(wc->imm_data);
			req_datagram->info_type = MICA_SET_REQUEST;

			//ERROR_LOG("IBV_WC_RECV_RDMA_WITH_IMM [%d]", partition_id);

			// 多线程分区处理
			distribute_partition_resp(peer_partition_id, rdma_trans, msg, start.tv_sec, start.tv_nsec);

			// 由于 mirror 收到 imm 报文不需要保存接收缓冲区，因为可以立刻释放接收缓冲区
			dhmp_post_recv(rdma_trans, task_ptr->sge.addr, recv_partition_id);
			//  = dhmp_mica_set_request_handler(rdma_trans, NULL, ntohl(wc->imm_data));
			
			break;
		case IBV_WC_RECV:
			msg = (struct dhmp_msg *) malloc(sizeof(struct dhmp_msg));
			/*read the msg content from the task_ptr sge addr*/
			msg->msg_type=*(enum dhmp_msg_type*)task_ptr->sge.addr;
			msg->data_size=*(size_t*)(task_ptr->sge.addr+sizeof(enum dhmp_msg_type));
			msg->data= task_ptr->sge.addr + sizeof(enum dhmp_msg_type) + sizeof(size_t);
			// 以下数据不会从报文中获取
			INIT_LIST_HEAD(&msg->list_anchor);
			msg->trans = rdma_trans;
			msg->recv_partition_id = recv_partition_id;

			//MICA_TIME_COUNTER_INIT();
			clock_gettime(CLOCK_MONOTONIC, &start);
			dhmp_wc_recv_handler(rdma_trans, msg, &is_async,start.tv_sec, start.tv_nsec);
			// dhmp_post_recv 需要放到多线程的末尾去处理
			// 发送双边操作的数据大小不能超过  SINGLE_NORM_RECV_REGION （16MB）
			if (! is_async)
			{
				dhmp_post_recv(rdma_trans, task_ptr->sge.addr, recv_partition_id);
				free(msg);
			}
			//MICA_TIME_COUNTER_CAL("dhmp_wc_recv_handler");
			break;

		case IBV_WC_RDMA_WRITE:
#ifdef DHMP_MR_REUSE_POLICY
			// 如果该区域的内存小于RDMA_SEND_THREASHOLD，则回收（不是释放）该块注册内存，用于下一次的数据传输
			//INFO_LOG("reused addr is [%p]", task_ptr->sge.addr);
			if (!task_ptr->is_imm)
			{
				if (task_ptr->sge.length <= RDMA_SEND_THREASHOLD)
				{
					pthread_mutex_lock(&client_mgr->mutex_send_mr_list);
					list_add(&task_ptr->smr->send_mr_entry, &client_mgr->send_mr_list);
					pthread_mutex_unlock(&client_mgr->mutex_send_mr_list);
				}
			}
#endif
			// task_ptr->addr_info->write_flag=false;
			task_ptr->done_flag=true;
			break;
		case IBV_WC_RDMA_READ:
			task_ptr->done_flag=true;
			break;
		default:
			ERROR_LOG("unknown opcode:%s",
			            dhmp_wc_opcode_str(wc->opcode));
			break;
	}
}

/**
 *	dhmp_wc_error_handler:handle the error work completion.
 */
static void dhmp_wc_error_handler(struct ibv_wc* wc)
{
	if(wc->status==IBV_WC_WR_FLUSH_ERR)
	{
		// INFO_LOG("work request flush, retry.....");
	}
	else
	{
		ERROR_LOG("wc status is [%s], byte_len is [%u], opcode is [%s]", \
				ibv_wc_status_str(wc->status), wc->byte_len, dhmp_wc_opcode_str(wc->opcode));
		return;
	}

}

/**
 *	dhmp_comp_channel_handler:create a completion channel handler
 *  note:set the following function to the cq handle work completion
 *  epoll回调函数入口
 */
void dhmp_comp_channel_handler(struct dhmp_cq* dcq)
{
	// struct dhmp_cq* dcq =(struct dhmp_cq*) data;
	struct ibv_cq* cq;
	void* cq_ctx;
	struct ibv_wc wc;
	int err=0;

	while(true)
	{
		if (*dcq->stop_flag_ptr == true)
		{
			INFO_LOG("dhmp_comp_channel_handler thread exit!");
			pthread_exit(0);
		}

		// //while(ibv_get_cq_event(dcq->comp_channel, &cq, &cq_ctx));
		// err=ibv_get_cq_event(dcq->comp_channel, &cq, &cq_ctx);
		// if(err)
		// {
		// 	//ERROR_LOG("ibv get cq event error.");
		// 	continue;
		// }

		// ibv_ack_cq_events(dcq->cq, 1);
		// err=ibv_req_notify_cq(dcq->cq, 0);
		// if(err)
		// {
		// 	//ERROR_LOG("ibv req notify cq error.");
		// 	continue;
		// }

		while(ibv_poll_cq(dcq->cq, 1, &wc))
		{
			if(wc.status==IBV_WC_SUCCESS)
				dhmp_wc_success_handler(&wc);
			else
				dhmp_wc_error_handler(&wc);
		}
	}
}

void* busy_wait_cq_handler(void* data)
{
	struct dhmp_cq* dcq = (struct dhmp_cq* )data;
	dhmp_comp_channel_handler(dcq);
}