#!/bin/bash
###
 # @Author: your name
 # @Date: 2022-02-22 22:09:51
 # @LastEditTime: 2022-02-22 23:09:09
 # @LastEditors: Please set LastEditors
 # @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 # @FilePath: /star_midd/launch.sh
### 

PROCESS_NUMS=4

PIDS_ARRAY=()
COUNTER=0
# $$ 表示当前进程的 pid 

whoami

rm -f *.log 
while [ $COUNTER -lt $PROCESS_NUMS ]
do
    nohup ./bin/mica  >"$COUNTER.log" 2>&1 &
    echo "Launch mica pid[$!]"
    PIDS_ARRAY=("${PIDS_ARRAY[@]}" "$!")
    let COUNTER+=1
done

chown gtwang *.log 
# PID=$(ps -ef | grep mica | grep -v grep | awk '{ print $2 }')
sleep 15
for PID in ${PIDS_ARRAY[@]} 
do
# -z 字符串长度为0时为真
    if [ -z "$PID" ]
    then
        echo "PID is ERROR!"
    else
        echo "kill mica pid[$PID]"
        kill $PID
    fi
done