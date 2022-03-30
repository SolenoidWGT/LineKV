/*
 * @Author: your name
 * @Date: 2022-03-09 20:00:04
 * @LastEditTime: 2022-03-09 21:04:46
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: /LineKV/src/mica_kv/benchmark.c
 */

#include "hash.h"
#include "dhmp.h"
#include "dhmp_log.h"
#include "midd_mica_benchmark.h"

int __test_size;
int __access_num=0;
int read_num, update_num;
int end_round=0;
bool get_is_more;
int op_gaps[4];
int little_idx;
bool is_all_set_all_get =false;

enum WORK_LOAD_DISTRIBUTED workload_type;

int main_node_is_readable;
const double A = 1.3;  
const double C = 1.0;  
double pf[TEST_KV_NUM]; 
int rand_num[TEST_KV_NUM]={0};
struct test_kv kvs_group[TEST_KV_NUM];

// 生成符合Zipfian分布的数据
void generate_zipfian()
{
    int i;
    double sum = 0.0;
 
    for (i = 0; i < TEST_KV_NUM; i++)
        sum += C/pow((double)(i+2), A);

    for (i = 0; i < TEST_KV_NUM; i++)
    {
        if (i == 0)
            pf[i] = C/pow((double)(i+2), A)/sum;
        else
            pf[i] = pf[i-1] + C/pow((double)(i+2), A)/sum;
    }
}

// 根据Zipfian分布生成索引
void pick_zipfian(int max_num)
{
	int i, index;

    generate_zipfian();

    srand(time(0));
    for ( i= 0; i < max_num; i++)
    {
        index = 0;
        double data = (double)rand()/RAND_MAX; 
        while (index<(max_num)&&data > pf[index])   
            index++;
		rand_num[i]=index;
    }
}

void pick_uniform(int max_num)
{
	int i, rand_idx, tmp;

    for (i=0 ;i<max_num; i++)
        rand_num[i] = i;

    srand(time(0));
    for ( i= 0; i < max_num; i++)
    {
        rand_idx = rand() % max_num; 
        tmp = rand_num[i];
        rand_num[i] = rand_num[rand_idx];
        rand_num[rand_idx] = tmp;
    }
}

struct test_kv *
generate_test_data(size_t key_offset, size_t val_offset, size_t value_length, size_t kv_nums)
{
    size_t i,j;
    // int partition_id;
    // struct test_kv *kvs_group;
    // kvs_group = (struct test_kv *) malloc(sizeof(struct test_kv) * kv_nums);
    memset(kvs_group, 0, sizeof(struct test_kv) * kv_nums);

    for (i = 0; i < kv_nums; i++)
    {
        size_t key = i;
        key = key<<16;
        // size_t key = 314156;
        // size_t value = i + offset;
        // uint64_t key_hash = hash((const uint8_t *)&key, sizeof(key));
        // value_length = sizeof(value) > value_length ? sizeof(value) : value_length;

        kvs_group[i].true_key_length = sizeof(key);
        kvs_group[i].true_value_length = value_length;
        kvs_group[i].key = (uint8_t *)malloc(kvs_group[i].true_key_length);
        kvs_group[i].value = (uint8_t*) malloc(kvs_group[i].true_value_length);
        kvs_group[i].key_hash = hash(kvs_group[i].key, kvs_group[i].true_key_length );

        // 注意我们 get 回来的数据需要  考虑到 header 和 tail 的大小
        for (j=0; j<1; j++) // 暂时只开一个缓冲区
            kvs_group[i].get_value[j] = (uint8_t*) malloc(kvs_group[i].true_value_length + VALUE_HEADER_LEN + VALUE_TAIL_LEN);

        memset(kvs_group[i].value, (int)(i+val_offset), kvs_group[i].true_value_length);
        memcpy(kvs_group[i].key, &key, kvs_group[i].true_key_length);
        // memcpy(kvs_group[i].value, &value, kvs_group[i].true_value_length);
        
        // partition_id = *((size_t*)kvs_group[i].key)  % (PARTITION_NUMS);
        //ERROR_LOG("Hash code %x, partition_id: [%d]", kvs_group[i].key_hash, partition_id);
    }

    return kvs_group;
}

	// size_t 	 out_value_length; 	// 返回值
	// uint32_t out_expire_time;	// 返回值
	// bool	 partial;			// 返回值
	// uint8_t  out_value[0];		// 返回值

// 我们不回去比较key，因为如果value可以正确拿到，则key一定是正确的（另外我们没用拿key的接口)
bool 
cmp_item_value(size_t a_value_length, const uint8_t *a_out_value, size_t b_value_length,const uint8_t *b_out_value)
{
    bool re = true;
    if (a_value_length != b_value_length)
    {
        ERROR_LOG("MICA value length error! %lu != %lu", a_value_length, b_value_length);
        re= (false);
    }

#ifdef DUMP_MEM
    size_t off = 0;
    bool first = false, second = false, second_count=0;
    for (off = 0; off < b_value_length; off++)
    {
        if (a_out_value[off] != b_out_value[off])
        {
            if (first == false)
            {
                first = true;
                size_t tp = off - 16;
                for (; tp < off; tp++)
                    printf("%ld, %hhu, %hhu\n", tp, a_out_value[tp], b_out_value[tp]);
            }
            // 打印 unsigned char printf 的 格式是 %hhu
            printf("%ld, %hhu, %hhu\n", off, a_out_value[off], b_out_value[off]);
        }
        else
        {
            if (first == true && second == false && second_count < 16)
            {
                printf("%ld, %hhu, %hhu\n", off, a_out_value[off], b_out_value[off]);
                second_count ++;
                if (second_count == 16)
                    second = true;
            }
        }
    }
#endif

    if (memcmp(a_out_value, b_out_value, b_value_length) != 0 )
    {
        ERROR_LOG("value context error! %p, %p, len is %lu", a_out_value, b_out_value, b_value_length);
        re=  (false);
    }

    return re;
}

void dump_value_by_addr(const uint8_t * value, size_t value_length)
{
    uint64_t header_v, tail_v, value_count;
    
    bool dirty;

    header_v = *(uint64_t*) value;
    value_count = *(uint64_t*) (value + sizeof(uint64_t));
    tail_v = *(uint64_t*) (value + 2*sizeof(uint64_t) + GET_TRUE_VALUE_LEN(value_length));
    dirty = *(bool*)(value + 3*sizeof(uint64_t) + GET_TRUE_VALUE_LEN(value_length));

    // INFO_LOG("value header_v is %lu, value_count is %lu, tail_v is %lu, dirty is %d", header_v,value_count,tail_v, dirty);
#ifdef DUMP_VALUE
    const uint8_t * value_base  = (value + 2 * sizeof(uint64_t));
    HexDump(value_base, (int)(GET_TRUE_VALUE_LEN(value_length)), (int) value_base);
#endif
}


bool 
cmp_item_all_value(size_t a_value_length, const uint8_t *a_out_value, size_t b_value_length,const uint8_t *b_out_value)
{
    bool re = true;
    if (a_value_length != b_value_length)
    {
        ERROR_LOG("MICA value length error! %lu != %lu", a_value_length, b_value_length);
        re= (false);
    }
    size_t off = 0;
    for (off = 0; off < b_value_length; off++)
    {
        if (a_out_value[off] != b_out_value[off])
        {
            // 打印 unsigned char printf 的 格式是 %hhu
            printf("%ld, %hhu, %hhu\n", off, a_out_value[off], b_out_value[off]);
        }
    }
    if (memcmp(a_out_value, b_out_value, b_value_length) != 0 )
    {
        ERROR_LOG("value context error! %p, %p, len is %lu", a_out_value, b_out_value, b_value_length);

        // dump_value_by_addr(a_out_value, a_value_length);
        // dump_value_by_addr(b_out_value, b_value_length);
        re=  (false);
    }

    if (memcmp(GET_TRUE_VALUE_ADDR(a_out_value), GET_TRUE_VALUE_ADDR(b_out_value), GET_TRUE_VALUE_LEN(b_value_length)) != 0 )
    {
        ERROR_LOG("true value error!");
        re=  (false);
    }
    return re;
}
