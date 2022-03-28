/*
 * @Author: your name
 * @Date: 2022-03-09 20:03:05
 * @LastEditTime: 2022-03-09 20:04:28
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: /LineKV/include/midd_mica_benchmark.h
 */

#define TEST_KV_NUM 6
#define ACCESS_NUM __access_num

extern int __access_num;
extern int rand_num[TEST_KV_NUM];
extern int read_num, update_num;
extern enum WORK_LOAD_DISTRIBUTED workload_type;
extern uint64_t set_counts, get_counts;
extern int partition_set_count[PARTITION_MAX_NUMS];

extern int get_is_more;
extern int main_node_is_readable;
extern struct test_kv kvs_group[TEST_KV_NUM];
extern int op_gaps[4];
extern int little_idx;
extern int end_round;
extern bool is_all_set_all_get;
extern size_t SERVER_ID;
extern int avg_partition_count_num;

void pick_zipfian(int max_num);
void pick_uniform(int max_num);

enum WORK_LOAD_DISTRIBUTED
{
    UNIFORM,
    ZIPFIAN
};

struct test_kv * generate_test_data(size_t key_offset, size_t val_offset, size_t value_length, size_t kv_nums);
bool  cmp_item_value(size_t a_value_length, const uint8_t *a_out_value, size_t b_value_length,const uint8_t *b_out_value);
void dump_value_by_addr(const uint8_t * value, size_t value_length);
bool cmp_item_all_value(size_t a_value_length, const uint8_t *a_out_value, size_t b_value_length,const uint8_t *b_out_value);

