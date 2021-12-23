#ifndef DHMP_TOP_API_H
#define DHMP_TOP_API_H

#include "dhmp_mica_shm_common.h"
#include "dhmp_transport.h"
// API
void
micaserver_get_cliMR(struct replica_mappings  *resp_all_mapping, size_t node_id);

enum ack_info_state
mica_basic_ack_req(size_t target_id, enum ack_info_type ack_type);

int
mica_ask_nodeID_req(struct dhmp_transport* new_rdma_trans);

// shm init 相关
void
mehcached_node_init();

#endif