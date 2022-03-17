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

TASK_TYPE=$1
GLOABL_PIDS_ARRAY=()
TOTAL_NODE_NUMS=0
mulitiRun(){
    COUNTER=$1
    PROCESS_NUMS=$2
    BIN_NAME=$3

    while [ $COUNTER -lt $PROCESS_NUMS ]
    do
        # 不能写成如下形式，这样 $! 的pid 就不一样了
        # { nohup ./bin/mica;  }  >"$COUNTER.log" 2>&1  &
        if [ $BIN_NAME = "mica" ];then
            nohup "./bin/$BIN_NAME" $TOTAL_NODE_NUMS >"$COUNTER.$BIN_NAME.log" 2>&1  &
        else
            nohup "./bin/$BIN_NAME" $TOTAL_NODE_NUMS >"$COUNTER.$BIN_NAME.log" 2>&1  &
        fi
        # exec 2>> $COUNTER.log && ./bin/mica < in 2>&1 >> $COUNTER.log &
        echo "Launch mica pid[$!]"
        GLOABL_PIDS_ARRAY=("${GLOABL_PIDS_ARRAY[@]}" "$!")
        let COUNTER+=1
        let TOTAL_NODE_NUMS+=1
    done
    chown gtwang *.log 
}

killAllProcess(){
    for PID in ${GLOABL_PIDS_ARRAY[@]} 
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
}

launch_mica_LineKV_task(){
    # launch all server
    mulitiRun 0 4 "mica"
    # PID=$(ps -ef | grep mica | grep -v grep | awk '{ print $2 }')
    # wait for all server ready
    sleep 10

    # launch mica_client 
    mulitiRun 0 1 "cli_mica"
    # wait mica_client test finished!
    sleep 20
}

launch_mica_CRAQ_task(){
    mulitiRun 0 4 "mica"
    sleep 5
    mulitiRun 0 1 "cli_mica"
    sleep 20
}

# $$ 表示当前进程的 pid 
whoami
rm -f *.log 
rm -f *.core.*


if [[ $TASK_TYPE = "LineKV" ]];
then
    echo "LineKV";
    launch_mica_LineKV_task
    killAllProcess
elif [[ $TASK_TYPE = "CRAQ" ]];
then
    echo "CRAQ";
    launch_mica_CRAQ_task
    killAllProcess
elif [[ $TASK_TYPE = "ZAB" ]];
then
    echo "ZAB";
else
    echo "Unkonwn task type!, exit";
fi

echo "Exit!"