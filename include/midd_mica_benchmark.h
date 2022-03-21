/*
 * @Author: your name
 * @Date: 2022-03-09 20:03:05
 * @LastEditTime: 2022-03-09 20:04:28
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: /LineKV/include/midd_mica_benchmark.h
 */

#define TEST_KV_NUM 64
#define ACCESS_NUM __access_num

extern int __access_num;
extern int rand_num[TEST_KV_NUM];

void pick_zipfian(int max_num);
void pick_uniform(int max_num);

enum WORK_LOAD_DISTRIBUTED
{
    UNIFORM,
    ZIPFIAN
};

struct test_kv * generate_test_data(size_t key_offset, size_t val_offset, size_t value_length, size_t kv_nums, size_t node_nums);
bool  cmp_item_value(size_t a_value_length, const uint8_t *a_out_value, size_t b_value_length,const uint8_t *b_out_value);
void dump_value_by_addr(const uint8_t * value, size_t value_length);
bool cmp_item_all_value(size_t a_value_length, const uint8_t *a_out_value, size_t b_value_length,const uint8_t *b_out_value);

