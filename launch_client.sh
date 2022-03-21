#!/bin/bash
###
 # @Author: your name
 # @Date: 2022-02-22 22:09:51
 # @LastEditTime: 2022-02-22 23:09:09
 # @LastEditors: Please set LastEditors
 # @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 # @FilePath: /star_midd/launch.sh
### 
export LIBC_FATAL_STDERR_=1

HOSTNAME=$(hostname)
echo $HOSTNAME

#测试数据类型
TASK_TYPE=$1
#node ID
NODE_ID=$2
# 分区数量
THREAD_NUM=$3
# 测试value大小 单位Byte
TEST_VALUE_SIZE=$4
# 数据分布
TEST_DISTRIBUTED=$5
# 测试次数
ACESSS_NUM=$7
# 读写比例
TEST_RW_RATE=$6


echo "Task: $TASK_TYPE, Node ID: $NODE_ID, Partition_num: $THREAD_NUM, Value_size: $TEST_VALUE_SIZE, $TEST_DISTRIBUTED, $TEST_RW_RATE"

nohup "./bin/$BIN_NAME" $TOTAL_NODE_NUMS $THREAD_NUM $TEST_VALUE_SIZE $TEST_DISTRIBUTED $ACESSS_NUM $TEST_RW_RATE>"$COUNTER.$BIN_NAME.log" 2>&1  &

PID=$!

sleep 20

kill $PID
echo "kill mica_client pid[$PID]"

