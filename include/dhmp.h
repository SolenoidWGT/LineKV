#ifndef DHMP_H
#define DHMP_H

#include "dhmp_mica_shm_common.h"

#define DHMP_CACHE_POLICY
#define DHMP_MR_REUSE_POLICY

#define DHMP_SERVER_DRAM_TH ((uint64_t)1024*1024*1024*1)

#define DHMP_SERVER_NODE_NUM 10

#define DHMP_DEFAULT_SIZE 256
#define DHMP_DEFAULT_POLL_TIME 800000000

#define DHMP_MAX_OBJ_NUM 40000
#define DHMP_MAX_CLIENT_NUM 100

#define PAGE_SIZE 4096
#define NANOSECOND (1000000000)

#define DHMP_RTT_TIME (6000)
#define DHMP_DRAM_RW_TIME (260)

#define max(a,b) (a>b?a:b)
#define min(a,b) (a>b?b:a)


#define DHMP_MR_REUSE_POLICY
#ifdef DHMP_MR_REUSE_POLICY
#define RDMA_SEND_THREASHOLD 2097152
#endif

#define POST_SEND_BUFFER_SIZE 1024*1024*128

// 集群中的 node id 编号宏
#define MAIN_NODE_ID 0
#define MIRROR_NODE_ID 1
#define REPLICA_NODE_HEAD_ID 2
#define REPLICA_NODE_TAIL_ID (server_instance->node_nums -1)
#define REPLICA_NODE_NUMS (server_instance->node_nums - 2)

extern struct memkind * pmem_kind;

// Replica REPLICA
// 如果只有一个副本节点，那么这个副本节点既是 REPLICA 又是 MIDDLE 还是 TAIL
enum dhmp_node_class {
	MAIN,
	MIRROR,
	REPLICA
};

#define IS_MAIN(type)    (type & (1 << MAIN) )
#define IS_MIRROR(type)  (type & (1 << MIRROR) )
#define IS_REPLICA(type) (type & (1 << REPLICA) )
// #define IS_HEAD(type) (type & (1 << HEAD) )
// #define IS_MIDDLE(type) (type & (1 << MIDDLE) )
// #define IS_TAIL(type) (type & (1 << TAIL) )

#define SET_MAIN(type) 	  ( type = (type | (1 << MAIN)   ) )
#define SET_MIRROR(type)  ( type = (type | (1 << MIRROR) ) )
#define SET_REPLICA(type) ( type = (type | (1 << REPLICA) ) )
// #define SET_HEAD(type)   ( type = (type | (1 << HEAD)   ) )
// #define SET_MIDDLE(type) ( type = (type | (1 << MIDDLE) ) )
// #define SET_TAIL(type)   ( type = (type | (1 << TAIL)   ) )


enum dhmp_msg_type{
	// DHMP_MSG_MALLOC_REQUEST,
	// DHMP_MSG_MALLOC_RESPONSE,
	// DHMP_MSG_MALLOC_ERROR,
	// DHMP_MSG_FREE_REQUEST,
	// DHMP_MSG_FREE_RESPONSE,
	// DHMP_MSG_APPLY_DRAM_REQUEST,
	// DHMP_MSG_APPLY_DRAM_RESPONSE,
	// DHMP_MSG_CLEAR_DRAM_REQUEST,
	// DHMP_MSG_CLEAR_DRAM_RESPONSE,
	// DHMP_MSG_MEM_CHANGE,
	// DHMP_MSG_SERVER_INFO_REQUEST,
	// DHMP_MSG_SERVER_INFO_RESPONSE,
	
	// DHMP_MSG_SEND_REQUEST,
	// DHMP_MSG_SEND_RESPONSE,
	DHMP_MSG_CLOSE_CONNECTION,

	/* WGT: add new msg type */
	DHMP_MICA_SEND_INFO_REQUEST,
	DHMP_MICA_SEND_INFO_RESPONSE,
};


enum ack_info_type{
	MICA_INIT_ADDR_ACK,
};

enum ack_info_state{
	MICA_ACK_INIT_ADDR_OK,
	MICA_ACK_INIT_ADDR_NOT_OK,
};

enum mica_send_info_type{
	MICA_GET_CLIMR_REQUEST,
	MICA_GET_CLIMR_RESPONSE,
	MICA_ACK_REQUEST,
	MICA_ACK_RESPONSE,
	MICA_SERVER_GET_CLINET_NODE_ID_REQUEST,
	MICA_SERVER_GET_CLINET_NODE_ID_RESPONSE,
};

enum middware_state{
	middware_INIT,
	middware_WAIT_MAIN_NODE,
	middware_WAIT_SUB_NODE,
	middware_WAIT_MATE_DATA,

};

enum request_state{
	RQ_INIT_STATE,
	RQ_BUFFER_STATE,
};

enum response_state
{
	RS_INIT_READY,
	RS_INIT_NOREADY,
	RS_BUFFER_READY,
	RS_BUFFER_NOREADY,
};

/*struct dhmp_msg:use for passing control message*/
struct dhmp_msg{
	enum dhmp_msg_type msg_type;
	size_t data_size;		// 整个报文的长度（包含 post_datagram header）
	void *data;
};

/*struct dhmp_addr_info is the addr struct in cluster*/
struct dhmp_addr_info{
	int read_cnt;
	int write_cnt;
	int node_index;
	bool write_flag;
	struct ibv_mr dram_mr;
	struct ibv_mr nvm_mr;
	struct hlist_node addr_entry;
};
struct dhmp_dram_info{
	void *nvm_addr;
	struct ibv_mr dram_mr;
};

/*
 * 一次dhmp标准的rpc通信过程是，主动发起请求的一方（客户端）构建request结构体，结构体中包含最终对端返回数据存放位置的指针，
 * 而接受请求的一方（服务端），构建response结构体，结构体中包含服务的返回的数据
 * 最终在客户端的recv_handler函数中将response结构体中的结果数据拷贝到request结构体给出的指针的地址处
 * 这也就是为什么response结构体里面需要包含request结构体 
 */

// 通用消息结构体
struct post_datagram
{
	struct post_datagram* req_ptr;	    	// request 报文的回调指针，用于发送方辨别自己发送的消息
	struct post_datagram* resp_ptr;			// response 报文的回调指针，用于接收方辨别自己发送的消息
	int    node_id;							// 身份标识，用于通信双方辨别发送方身份，一次rpc过程中node_id 改变两次
	enum mica_send_info_type info_type;		// 报文类型的判别
	size_t info_length;						// 具体消息报文的长度
	bool   done_flag;						// 用于判别报文是否发送完成，用于阻塞
};
#define HEADER_LEN sizeof(struct post_datagram)
#define DATAGRAM_ALL_LEN(len) ((HEADER_LEN) + len)
#define DATA_ADDR(start_addr, offset) ((char *)start_addr +  HEADER_LEN + offset)

// 下面这个消息用于处理MICA服务端向MICA客户端发送的具体消息报文
// 包含 init_addr, update , insert, delete 等操作
struct dhmp_mica_get_cli_MR_request
{
	struct replica_mappings  *info_revoke_ptr;	// 回调指针
};
struct dhmp_mica_get_cli_MR_response
{
	struct dhmp_mica_get_cli_MR_request	request;
	struct replica_mappings  resp_all_mapping;	// 完整数据结构体		 	 
};
struct dhmp_mica_ack_request
{
	enum ack_info_type ack_type;	
};
struct dhmp_mica_ack_response
{
	enum ack_info_state ack_state;		 	 
};

struct dhmp_get_nodeID_request
{
	int node_id;	
};
struct dhmp_get_nodeID_response
{	
	int resp_node_id;	 	 
};

extern pthread_mutex_t buff_init_lock; 
extern int wait_work_counter;
extern int wait_work_expect_counter;

#endif
