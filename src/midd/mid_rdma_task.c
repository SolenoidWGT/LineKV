#include "dhmp.h"
#include "dhmp_log.h"
#include "dhmp_config.h"
#include "dhmp_context.h"
#include "dhmp_dev.h"
#include "dhmp_transport.h"
#include "dhmp_task.h"

#include "dhmp_log.h"
#include "util.h"
static void
rdma_trans_send_mr_lock(struct dhmp_transport* rdma_trans)
{
	while (1)
	{
		if (__sync_bool_compare_and_swap((volatile uint64_t *)&(rdma_trans->send_mr_lock), 0UL, 1UL))
			break;
	}
}

static void
rdma_trans_send_mr_unlock(struct dhmp_transport* rdma_trans)
{
	memory_barrier();
	*(volatile uint64_t *)&(rdma_trans->send_mr_lock) = 0UL;
}

struct dhmp_task* 
dhmp_recv_task_create(struct dhmp_transport* rdma_trans, 
										void *addr )
{
	struct dhmp_task *recv_task;

	// memory leak ?
	recv_task=malloc(sizeof(struct dhmp_task));
	if(!recv_task)
	{
		ERROR_LOG("allocate memory error.");
		return NULL;
	}

	/*init the recv task*/
	recv_task->sge.addr=addr;
	
	/*according to the flag of is_poll_qp,
	 decide to post what size sge*/
	recv_task->sge.length=SINGLE_NORM_RECV_REGION;
	
	recv_task->sge.lkey=rdma_trans->recv_mr.mr->lkey;
	recv_task->rdma_trans=rdma_trans;

	return recv_task;
}

struct dhmp_task* dhmp_send_task_create(struct dhmp_transport* rdma_trans,
										struct dhmp_msg *msg)
{
	struct dhmp_task *send_task;
	struct dhmp_mr *send_mr=&rdma_trans->send_mr;
	
	// memory leak ?
	send_task=malloc(sizeof(struct dhmp_task));
	if(!send_task)
	{
		ERROR_LOG("allocate memory error.");
		return NULL;
	}

	send_task->sge.length=sizeof(enum dhmp_msg_type)+
							sizeof(size_t)+
							msg->data_size;

	// 目前dhmp中post send 使用的是一个环形缓冲区
	if(send_mr->cur_pos+send_task->sge.length>SEND_REGION_SIZE)
	{
		INFO_LOG("dhmp reuse send_buffer!");
		send_mr->cur_pos=0;
	}

	send_task->sge.addr=send_mr->addr+send_mr->cur_pos;
	send_task->sge.lkey=send_mr->mr->lkey;
	send_task->rdma_trans=rdma_trans;
	
	send_mr->cur_pos+=send_task->sge.length;
	
	/*use msg build send task*/
	memcpy(send_task->sge.addr, &msg->msg_type, sizeof(enum dhmp_msg_type));
	memcpy(send_task->sge.addr+sizeof(enum dhmp_msg_type), 
			&msg->data_size, sizeof(size_t));
	memcpy(send_task->sge.addr+sizeof(enum dhmp_msg_type)+sizeof(size_t), 
			msg->data, msg->data_size);

	return send_task;

}

struct dhmp_task* dhmp_read_task_create(struct dhmp_transport* rdma_trans,
										struct dhmp_send_mr *smr,
										int length)
{
	struct dhmp_task *read_task;
	
	read_task=malloc(sizeof(struct dhmp_task));
	if(!read_task)
	{
		ERROR_LOG("allocate memory error.");
		return NULL;
	}
	
	read_task->done_flag=false;
	read_task->rdma_trans=rdma_trans;
	read_task->sge.addr=smr->mr->addr;
	read_task->sge.length=length;
	read_task->sge.lkey=smr->mr->lkey;
	read_task->smr=smr;

	return read_task;
}


struct dhmp_task* dhmp_write_task_create(struct dhmp_transport* rdma_trans,
										struct dhmp_send_mr *smr,
										int length)
{
	struct dhmp_task *write_task;
	
	write_task=malloc(sizeof(struct dhmp_task));
	if(!write_task)
	{
		ERROR_LOG("allocate memory error.");
		return NULL;
	}
	
	write_task->done_flag=false;
	write_task->rdma_trans=rdma_trans;
	write_task->sge.addr=smr->mr->addr;
	write_task->sge.length=length;
	write_task->sge.lkey=smr->mr->lkey;
	write_task->smr=smr;

	return write_task;
}

// TODO 更高效的双边 send_mr 管理
// void * mica_get_trans_mr_addr(struct dhmp_transport* rdma_trans)
// {
// 	struct dhmp_task *send_task;
// 	struct dhmp_mr *send_mr=&rdma_trans->send_mr;

// 	rdma_trans_send_mr_lock(rdma_trans);
// 	// 目前dhmp中post send 使用的是一个环形缓冲区
// 	if(send_mr->cur_pos+send_task->sge.length>SEND_REGION_SIZE)
// 	{
// 		INFO_LOG("dhmp reuse send_buffer!");
// 		send_mr->cur_pos=0;
// 	}

// 	send_task->sge.addr=send_mr->addr+send_mr->cur_pos;
// 	send_task->sge.lkey=send_mr->mr->lkey;
// 	send_task->rdma_trans=rdma_trans;
	
// 	send_mr->cur_pos+=send_task->sge.length;
	
// 	/*use msg build send task*/
// 	memcpy(send_task->sge.addr, &msg->msg_type, sizeof(enum dhmp_msg_type));
// 	memcpy(send_task->sge.addr+sizeof(enum dhmp_msg_type), 
// 			&msg->data_size, sizeof(size_t));
// 	memcpy(send_task->sge.addr+sizeof(enum dhmp_msg_type)+sizeof(size_t), 
// 			msg->data, msg->data_size);

// 	return send_task;
// }