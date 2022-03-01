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
// #define offsetof(TYPE, MEMBER) ((size_t) &((TYPE *)0)->MEMBER)

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
	REPLICA,
	HEAD,
	TAIL,

	CLIENT
};


#define ONLY_MAIN_NODE_W	
// #define ALL_NODE_W

#define IS_MAIN()    (server_instance->server_type & (uint8_t)(1 << MAIN) )
#define IS_MIRROR()  (server_instance->server_type & (uint8_t)(1 << MIRROR) )
#define IS_REPLICA() (server_instance->server_type & (uint8_t)(1 << REPLICA) )
// head 是副本节点中的头节点（因为严格意义上来说主节点是头节点）
#define IS_HEAD() 	 (server_instance->server_type & (uint8_t)(1 << HEAD) )
// #define IS_MIDDLE(type) (type & (1 << MIDDLE) )
#define IS_TAIL() 	 (server_instance->server_type & (uint8_t)(1 << TAIL) )

#define IS_CLIENT()  (server_instance->server_type & (uint8_t)(1 << CLIENT) )

#define SET_MAIN() 	  ( server_instance->server_type = (server_instance->server_type | (uint8_t)(1 << MAIN)   ) )
#define SET_MIRROR()  ( server_instance->server_type = (server_instance->server_type | (uint8_t)(1 << MIRROR) ) )
#define SET_REPLICA() ( server_instance->server_type = (server_instance->server_type | (uint8_t)(1 << REPLICA) ) )
#define SET_HEAD()    ( server_instance->server_type = (server_instance->server_type | (uint8_t)(1 << HEAD)   ) )
// #define SET_MIDDLE(type) ( type = (type | (1 << MIDDLE) ) )
#define SET_TAIL()     ( server_instance->server_type = (server_instance->server_type | (uint8_t)(1 << TAIL)   ) )

#define SET_CLIENT()   ( server_instance->server_type = (server_instance->server_type | (uint8_t)(1 << CLIENT)   ) )

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
	MICA_SET_REQUEST,
	MICA_SET_RESPONSE,

	MICA_GET_REQUEST,
	MICA_GET_RESPONSE,

	MICA_REPLICA_UPDATE_REQUEST,
	MICA_REPLICA_UPDATE_RESPONSE,
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
	volatile bool   done_flag;						// 用于判别报文是否发送完成，用于阻塞
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

/*
uint8_t current_alloc_id, struct mehcached_table *table, uint64_t key_hash,\
                const uint8_t *key, size_t key_length, const uint8_t *value, size_t value_length,\
                uint32_t expire_time, bool overwrite
*/
struct dhmp_mica_set_request
{
	uint8_t current_alloc_id;
	// struct mehcached_table *table; // don't needed
	struct dhmp_mica_set_request * job_callback_ptr;
	uint64_t	key_hash;
	size_t 		key_length;
	size_t 		value_length;
	// const uint8_t *key;		// don't needed
	// const uint8_t *value;	// don't needed
	uint32_t 	expire_time;
	bool 		overwrite;
	bool		is_update;
	bool 		is_success;					// 返回值，如果为false需要重试
	size_t 		out_mapping_id;				// 远端set后对应的item的 mapping_Id，返回值
	uintptr_t   out_value_addr;				// 远端set后对应的item value 的虚拟地址，返回值
	// key and value segment begin
	uint8_t data[0];
};

struct dhmp_mica_set_response
{
	size_t 		out_mapping_id;
	uintptr_t   value_addr;
	// 如果是主节点则不需要用到下面的字段
	uint64_t 	key_hash;
	size_t 		key_length;
	bool 		is_success;
	uint8_t 	key_data[0];
};

struct set_requset_pack
{
	struct post_datagram * req_ptr;
	struct dhmp_mica_set_request * req_info_ptr;
};

#define MICA_DEFAULT_VALUE_LEN (1024*1024)
struct dhmp_mica_get_request
{
	uint8_t current_alloc_id;
	struct dhmp_mica_get_request * job_callback_ptr;
	uint64_t key_hash;
	size_t   key_length;
	struct dhmp_mica_get_response * get_resp;
	uint8_t data[0];		//返回值是 dhmp_mica_get_response
};

struct dhmp_mica_get_response
{
	size_t 	 out_value_length; 	// 返回值  8 B
	bool	 partial;			// 返回值  1 B + 3B(padding)
	uint32_t out_expire_time;	// 返回值  4 B

	// uint8_t  out_value[MICA_DEFAULT_VALUE_LEN];		// 返回值
	uint8_t  out_value[0];	
};
struct dhmp_write_request
{
	struct dhmp_transport* rdma_trans;
	struct ibv_mr* mr;
	void* local_addr;
	size_t length;
	uintptr_t remote_addr;
};

// 重点，更新操作的实现
// dhmp_update_request 应该是在副本节点解析完 dhmp_mica_set_request
// ，查找hash表之后生成的，其用于传递给nic线程进行value写操作的offload
// 需要在 mehcached_item 中添加一个锁，防止在 nic 进行写操作的时候 value
// 就被删除了
struct dhmp_update_request
{
	struct mehcached_item * item;
	uint64_t item_offset;
	struct dhmp_write_request write_info;
	struct list_head sending_list;
};


// 增加一个双边操作用于通知，模仿 hyperloop 的行为
// 只需要向下游节点发送一个偏移量即可？ 是否安全？？
struct dhmp_update_notify_request
{
	// (struct mehcached_item *)mehcached_dynamic_item(&table->alloc, item_offset);
	// 我们知道了 item_offset 就可以确定 item
	uint64_t item_offset;
};

struct dhmp_update_notify_response
{
	bool is_success;
};

extern pthread_mutex_t buff_init_lock; 
extern int wait_work_counter;
extern int wait_work_expect_counter;

void dump_mr(struct ibv_mr * mr);
int dhmp_rdma_write_packed (struct dhmp_write_request * write_req);
void mica_replica_update_notify(uint64_t item_offset);
extern volatile bool replica_is_ready;

// 最大超时时间，1s, 单位ns
// 单线程 reactor 模式中的 recv 操作（比如接收方的get操作）一定不能阻塞，否则会发生死锁
#define TIMEOUT_LIMIT 1000000000
#define MICA_TIME_COUNTER_INIT() clock_gettime(CLOCK_MONOTONIC, &SP_start);					

#define MICA_TIME_COUNTER_CAL()								\
	{														\
		clock_gettime(CLOCK_MONOTONIC, &SP_end);			\
		mica_total_time = (((SP_end.tv_sec * 1000000000) + SP_end.tv_nsec) - ((SP_start.tv_sec * 1000000000) + SP_start.tv_nsec)) / TIMEOUT_LIMIT; \
		{													\
			if (mica_total_time > 0)						\
			{												\
				ERROR_LOG("TIMEOUT!, exit");				\
				Assert(false);								\
			}												\
		}while(0);											\
	}while(0);


extern struct mehcached_table *log_table;


struct test_kv
{
	uint8_t * key;
	uint8_t * value;
	size_t true_key_length;
	size_t true_value_length;
	uint8_t   value_checksum;
	struct mehcached_item * item;	// 如果有
};

void dump_value_by_addr(const uint8_t * value, size_t value_length);
// TODO : 新的mapping增加后的通知机制
// TODO : logtable 的垃圾回收
// TODO : 去掉 #define MICA_DEFAULT_VALUE_LEN (1024)
// TODO : 边长 key 插入， header ,tail 元数据的迁移


#define ONE_LOOP_TIMER
#define PARTITION_NUM 16
#define PARTITION_ID(key_hash) ((uint16_t) (uint16_t)(key_hash >> 48) & (uint16_t)( PARTITION_NUM - 1))
#define TABLE_POOL_SIZE 1024*1024*1024*1
#define TABLE_BUCKET_NUMS 64

#endif
