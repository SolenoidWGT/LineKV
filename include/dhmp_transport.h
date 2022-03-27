#ifndef DHMP_TRANSPORT_H
#define DHMP_TRANSPORT_H
#include "dhmp_context.h"
#include "dhmp_dev.h"

#include "dhmp_task.h"

#define ADDR_RESOLVE_TIMEOUT 500
#define ROUTE_RESOLVE_TIMEOUT 500

#define RECV_REGION_SIZE (16*1024*1024)
#define SEND_REGION_SIZE (16*1024*1024)

// #define SINGLE_POLL_RECV_REGION_KV (64*1024)
// #define SINGLE_NORM_RECV_REGION_KV (64*1024)

/*recv region include poll recv region,normal recv region*/
// 由于我们初始化时候需要交换大量的注册地址信息，目前只是通过一个 send/recv 完成，如果
// recv_region 太小，就会报 IBV_WC_LOC_LEN_ERR (1) - Local Length Error 的错误
#define SINGLE_POLL_RECV_REGION (2*64*1024)
#define SINGLE_NORM_RECV_REGION (2*64*1024)

// 函数前缀没有双下划线的函数是单线程执行的
void dhmp_wc_recv_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, bool *is_async);


enum dhmp_transport_state {
	DHMP_TRANSPORT_STATE_INIT,
	DHMP_TRANSPORT_STATE_LISTEN,
	DHMP_TRANSPORT_STATE_CONNECTING,
	DHMP_TRANSPORT_STATE_CONNECTED,
	DHMP_TRANSPORT_STATE_DISCONNECTED,
	DHMP_TRANSPORT_STATE_RECONNECT,
	DHMP_TRANSPORT_STATE_CLOSED,
	DHMP_TRANSPORT_STATE_DESTROYED,
	DHMP_TRANSPORT_STATE_ERROR,

	/* WGT: add new transport state*/
	DHMP_TRANSPORT_STATE_REJECT,
	DHMP_TRANSPORT_STATE_ADDR_ERROR,
};

struct dhmp_cq{
	struct ibv_cq	*cq;
	struct ibv_comp_channel	*comp_channel;
	struct dhmp_device *device;
	
	/*add the fd of comp_channel into the ctx*/
	struct dhmp_context *ctx;
	bool * stop_flag_ptr;
};

struct dhmp_mr{
	size_t cur_pos;
	void *addr;
	struct ibv_mr *mr;
};

struct dhmp_transport{
	struct sockaddr_in	peer_addr;
	struct sockaddr_in	local_addr;
	
	int node_id;	/* peer node id*/
	enum dhmp_transport_state trans_state;
	struct dhmp_context *ctx;
	struct dhmp_device *device;
	struct dhmp_cq *dcq;
	struct ibv_qp *qp;
	struct rdma_event_channel *event_channel;
	struct rdma_cm_id	*cm_id;

	/*the var use for two sided RDMA*/
	struct dhmp_mr send_mr[PARTITION_MAX_NUMS+1];
	struct dhmp_mr recv_mr[PARTITION_MAX_NUMS+1];
	
	bool is_poll_qp;
	struct dhmp_transport *link_trans;

	long dram_used_size;
	long nvm_used_size;

	bool is_listen;	// 新增的 rdma_trans 标识，如果为true则表示该 trans 是一个 server监听trans
	bool is_active;	// 如果为 true 表示该节点是主动与对方建立连接
	struct rdma_conn_param  connect_params;		/* WGT */
	enum middware_state trans_mid_state;		/* WGT: mark this trans is at which middware stage */

	uint64_t send_mr_lock;
	uint64_t recv_mr_lock;
	struct list_head client_entry;
};

struct dhmp_send_mr{
	struct ibv_mr *mr;
	struct list_head send_mr_entry;
};


struct dhmp_cq* dhmp_cq_get(struct dhmp_device* device, struct dhmp_context* ctx);

struct dhmp_transport* dhmp_transport_create(struct dhmp_context* ctx, 
													struct dhmp_device* dev,
													bool is_listen,
													bool is_poll_qp,
													int peer_node_id);

// WGT
int free_trans(struct dhmp_transport* rdma_trans);


//void dhmp_transport_destroy(struct dhmp_transport *rdma_trans);


int dhmp_transport_connect(struct dhmp_transport* rdma_trans,
                             	const char* url, int port);

int dhmp_transport_listen(struct dhmp_transport* rdma_trans, int listen_port);


void dhmp_post_send(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg_ptr, size_t partition_id);


/**
 *	dhmp_post_all_recv:loop call the dhmp_post_recv function
 */
void dhmp_post_all_recv(struct dhmp_transport *rdma_trans);

void dhmp_post_recv(struct dhmp_transport* rdma_trans, void *addr, size_t partition_id);



int dhmp_rdma_read_after_write ( struct dhmp_transport* rdma_trans, struct dhmp_addr_info *addr_info, \
				struct ibv_mr* mr, void* local_addr, int length);

int dhmp_rdma_write ( struct dhmp_transport* rdma_trans,
							struct ibv_mr* mr, 
							void* local_addr, 
							size_t length,
							uintptr_t remote_addr);

int dhmp_rdma_read(struct dhmp_transport* rdma_trans, struct ibv_mr* mr, void* local_addr, int length, 
						off_t offset);

const char *  dhmp_printf_connect_state(enum dhmp_transport_state state);
int find_node_id_by_socket(struct sockaddr_in *sock);
struct dhmp_transport*  find_connect_client_by_nodeID(int node_id);
struct dhmp_transport*  find_connect_by_socket(struct sockaddr_in *sock);
struct dhmp_transport* find_connect_server_by_nodeID(int node_id);
struct dhmp_transport* dhmp_client_node_select_head();
extern int client_find_server_id();
extern int find_next_node(int id);

void dhmp_post_send_new(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg_ptr);
int dhmp_rdma_write_mica_warpper (struct dhmp_transport* rdma_trans,
						struct mehcached_item * item,
						struct ibv_mr* mr, 
						size_t length,
						uintptr_t remote_addr);

int mica_clinet_connect_server(int buffer_size, int target_id);
#endif

