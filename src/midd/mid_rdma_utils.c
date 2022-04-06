// _GNU_SOURCE 这个参数表示你编写符合 GNU 规范的代码，GNU 相对 POSIX 有一些增强，
// 也有一些缺少，总体来说 GNU 的实现应该是更好一点。但是这关系到软件的可移植性。
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sched.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "dhmp.h"
#include "dhmp_transport.h"

#include "dhmp_task.h"
#include "dhmp_client.h"
#include "dhmp_log.h"
#include "mid_rdma_utils.h"
#include <sys/select.h>



#define MAX_CPU_NUMS 128
static int local_sys_cpu_num = 0;
static char *cpu_set_map = NULL;

// 初始化 cpu_set 
void init_cpu_set_map()
{
	local_sys_cpu_num = sysconf(_SC_NPROCESSORS_CONF);
	Assert(local_sys_cpu_num > 0);
	cpu_set_map = (char *) malloc(sizeof(char) * local_sys_cpu_num);
	memset(cpu_set_map, '0', sizeof(char) * local_sys_cpu_num);
	INFO_LOG("Local sys cpu num is %d", local_sys_cpu_num);
}

int malloc_cpu()
{
	int i = 0;
	for (i = 0; i< local_sys_cpu_num; i++)
	{
		if (cpu_set_map[i] == '0')
			return i;
	}
	ERROR_LOG("not enough cpu!");
	return -1;
}

void free_cpu(int index)
{
	cpu_set_map[index] = 0;
}

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
