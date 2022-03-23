#ifndef DHMP_CONTEXT_H
#define DHMP_CONTEXT_H
#define DHMP_EPOLL_SIZE 1024

#ifndef ARRAY_SIZE
#define ARRAY_SIZE(arr) (sizeof(arr) / sizeof((arr)[0]))
#endif

#include <errno.h> 
#include "dhmp.h"
#include "mica_partition.h"
#define handle_error_en(en, msg) \
        do { errno = en; perror(msg); exit(EXIT_FAILURE); } while (0)

struct dhmp_mica_msg_data
{
	struct dhmp_msg* msg;
	struct dhmp_transport* rdma_trans;
	enum mica_send_info_type resp_type;
	volatile char set_tag;
};


typedef void (*dhmp_event_handler)(int fd, void *data_ptr);

struct dhmp_event_data{
	int fd;
	void *data_ptr;
	dhmp_event_handler event_handler;
};

struct dhmp_context{
	int epoll_fd;
	bool stop;
	pthread_t epoll_thread;
	bool stop_flag[MAX_CQ_NUMS];
	pthread_t busy_wait_cq_thread[MAX_CQ_NUMS];
};

struct mica_work_context{
	pthread_t threads[PARTITION_MAX_NUMS];

	// 为什么 mica 使用的是 int64 作为 cas ，而不是使用 char ， char 的读取难道不能保证原子吗，多核？单核？多CPU？
	volatile /*char*/ uint64_t  bit_locks[PARTITION_MAX_NUMS];

	struct dhmp_mica_msg_data buff_msg_data[PARTITION_MAX_NUMS];
};

int dhmp_context_init(struct dhmp_context *ctx);

int dhmp_context_add_event_fd(struct dhmp_context *ctx,
								int events,
								int fd,
								void *data_ptr,
								dhmp_event_handler event_handler);

int dhmp_context_del_event_fd(struct dhmp_context *ctx, int fd);

#endif

