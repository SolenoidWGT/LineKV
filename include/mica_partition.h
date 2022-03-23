#ifndef MICA_PARTITION
#define MICA_PARTITION

#define _GNU_SOURCE 1
#include <sched.h> 
#define PARTITION_MAX_NUMS 16
#define PARTITION_NUMS __partition_nums

extern unsigned long long  __partition_nums;
#endif