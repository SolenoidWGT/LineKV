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
				bool is_update);

struct dhmp_mica_get_response*
mica_get_remote_warpper(uint8_t current_alloc_id,  uint64_t key_hash, const uint8_t *key, 
				size_t key_length, 
				bool is_async, 
				struct set_requset_pack * req_callback_ptr,
				size_t target_id);
#endif