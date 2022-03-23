#ifndef DHMP_TOP_API_H
#define DHMP_TOP_API_H

#include "dhmp_mica_shm_common.h"
#include "dhmp_transport.h"
// API
void
micaserver_get_cliMR(struct replica_mappings  *resp_all_mapping, size_t node_id);

enum ack_info_state
mica_basic_ack_req(size_t target_id, enum ack_info_type ack_type, bool block);

int
mica_ask_nodeID_req(struct dhmp_transport* new_rdma_trans);

// shm init 相关

size_t
mica_set_remote_warpper(uint8_t current_alloc_id,  
				const uint8_t* no_header_key, uint64_t key_hash,size_t true_key_length, 
				const uint8_t* no_header_value, size_t true_value_length,
                uint32_t expire_time, bool overwrite, 
				bool is_async, 
				struct set_requset_pack * req_callback_ptr,
				size_t target_id,
				bool is_update,
				size_t self_node_id,
				size_t tag);

void
mica_get_remote_warpper(uint8_t current_alloc_id,  uint64_t key_hash, const uint8_t *key, 
				size_t key_length, 
				bool is_async, 
				struct set_requset_pack * req_callback_ptr,
				size_t target_id,
				size_t self_node_id,
				size_t expect_length,
				size_t tag,
				struct dhmp_mica_get_reuse_ptr *reuse_ptrs);


size_t
mica_set_remote(uint8_t current_alloc_id,  uint64_t key_hash, const uint8_t *key, 
				size_t key_length, const uint8_t *value, size_t value_length,
                uint32_t expire_time, bool overwrite, 
				bool is_async, 
				struct set_requset_pack * req_callback_ptr,
				size_t target_id,
				bool is_update,
				size_t self_node_id,
				size_t tag,
				bool re_use_datagram,
				void* req_base,
				size_t __total_length,
				size_t partition_id);

#endif