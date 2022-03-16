#include "dhmp.h"
#include "dhmp_transport.h"

#include "dhmp_task.h"
#include "dhmp_client.h"
#include "dhmp_log.h"
#include "mid_rdma_utils.h"
#include <sys/select.h>

int dhmp_memory_register(struct ibv_pd *pd, 
									struct dhmp_mr *dmr, size_t length)
{
	dmr->addr=malloc(length);
	if(!dmr->addr)
	{
		ERROR_LOG("allocate mr memory error.");
		return -1;
	}

	dmr->mr=ibv_reg_mr(pd, dmr->addr, length,  IBV_ACCESS_LOCAL_WRITE|
												IBV_ACCESS_REMOTE_READ|
												IBV_ACCESS_REMOTE_WRITE|
												IBV_ACCESS_REMOTE_ATOMIC);
	if(!dmr->mr)	
	{
		ERROR_LOG("rdma register memory error. register mem length is [%u], error number is [%d], reason is \"%s\"",  length, errno, strerror(errno));
		goto out;
	}

	dmr->cur_pos=0;
	return 0;

out:
	free(dmr->addr);
	return -1;
}

struct ibv_mr * dhmp_memory_malloc_register(struct ibv_pd *pd, size_t length, int nvm_node)
{
	struct ibv_mr * mr = NULL;
	void * addr= NULL;
	addr = numa_alloc_onnode(length, nvm_node);
	// dmr->addr=malloc(length);

	if(!addr)
	{
		ERROR_LOG("allocate mr memory error.");
		return NULL;
	}
	
	mr=ibv_reg_mr(pd, addr, length, IBV_ACCESS_LOCAL_WRITE|
									IBV_ACCESS_REMOTE_READ|
									IBV_ACCESS_REMOTE_WRITE|
									IBV_ACCESS_REMOTE_ATOMIC);
	if(!mr)
	{
		ERROR_LOG("rdma register memory error.");
		goto out;
	}
	mr->addr = addr;
	mr->length = length;
	return mr;
out:
	numa_free(addr, length);
	return NULL;
}

int bit_count(int id)
{
	int c = 0;
	while (id != 0)
	{
		id /= 10;
		c++;
	}
	return c;
}

void 
sleep_ms(unsigned int secs)
{
    struct timeval tval;
    tval.tv_sec=secs/1000;
    tval.tv_usec=(secs*1000)%1000000;
    select(0,NULL,NULL,NULL, &tval);
}