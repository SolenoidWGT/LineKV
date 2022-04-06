# LineKV
Chain-replicated distributed kv storage



### 分支

Whale分支：master

CRAQ分支：mica_CRAQ

CHT分支：mica_CHT

### 编译

```
# 不要使用root编译，否则scp可能会出现权限问题
# 修改脚本中的 chown -R gtwang 为自己
./rebuild.sh
```

### 发送

```
# 可以随便修改 ./send_all.sh 的发送位置
./send_all.sh
```

### 启动

```sh
<taskset -c 0-19> <./bin/mica> <0> <6> <0> <32> <uniform> <-1> <1.0>
解释：
<绑核> 
./bin/mica 
<节点编号（从0开始）> 
<线程数> 
<是否为ubuntu系统，node24为1，其余节点为0> 
<测试数据大小，单位B> 
<数据集类型 可选：[uniform|zipfian],其中zipfian是带有读惩罚机制，uniform没有> 
<废弃的操作数量，置为-1即可> 
<读写比例 可选：[1.0 | 0.75 | 0.5 | 0.2 | 0.05 | 0.01]>
```

7节点测试

```bash
taskset -c 0-19 ./bin/mica 0 6 0 32 uniform -1 1.0

taskset -c 0-19 ./bin/mica 1 6 0 32 uniform -1 1.0

taskset -c 0-19 ./bin/mica 2 6 0 32 uniform -1 1.0

# node24 为ubuntu节点，无法编译，直接拷贝可执行文件执行即可
taskset -c 0-19 ./mica 3 6 1 32 uniform -1 1.0

# 第五个以上的server绑核到编号 20 以上的cpu
taskset -c 20-39 ./bin/mica 4 6 0 32 uniform -1 1.0

taskset -c 20-39 ./bin/mica 5 6 0 32 uniform -1 1.0

taskset -c 20-39 ./mica 6 6 1 32 uniform -1 1.0
```

Whale和CHT节点启动顺序无要求，CRAQ要求先从尾节点到主节点依次启动。

工程里面的几个launch脚本还没测试写完，不要用。

### 修改测试类型

在dhmp.h头文件最下面有几个宏，这几个宏除了PERF_TEST  同一时刻只能开启一个

```c
// 生成perf测试，这个没什么用，就是主节点会无限下发任务让程序跑的时间长一点
// #define PERF_TEST

// 吞吐测试
#define THROUGH_TEST

// 没什么用
// #define MAIN_LOG_DEBUG_LATENCE

// 延迟测试
// #define DHMP_POST_SEND_LATENCE

// CPU高负载测试
//#define TEST_CPU_BUSY_WORKLOAD
```

