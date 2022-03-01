/*
 * @Author: your name
 * @Date: 2021-04-25 17:46:17
 * @LastEditTime: 2021-06-06 22:18:20
 * @LastEditors: Please set LastEditors
 * @Description: In User Settings Edit
 * @FilePath: /RDMA_middleware/include/mid_rdma_utils.h
 */
#ifndef MID_RDMA_UTILS_H
#define MID_RDMA_UTILS_H

#include "dhmp.h"
#include "dhmp_transport.h"


struct ibv_mr * dhmp_memory_malloc_register(struct ibv_pd *pd,  size_t length, int nvm_node);

int dhmp_memory_register(struct ibv_pd *pd, 
									struct dhmp_mr *dmr, size_t length);



extern int bit_count(int id);
void init_cpu_set_map();
uint8_t CheckXor(const uint8_t *strData, size_t len)  ;
#endif