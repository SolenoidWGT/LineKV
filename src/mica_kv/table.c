// Copyright 2014 Carnegie Mellon University
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "table.h"
#include "util.h"
#include "shm.h"
#include "dhmp_log.h"
#include <stdio.h>

MEHCACHED_BEGIN

// a test feature to deduplicate PUT requests within the same batch
//#define MEHCACHED_DEDUP_WITHIN_BATCH

// 注意在对 table_is_inited 进去写或读时，必须使用 volatile 或者 memory_barrier
// 否则会出现读到寄存器的旧值或者只写到寄存器的bug情况
// table_is_inited 被 volatile 修饰后所有对该变量的访问都是访问内存，会影响性能
// 所以使用 memory_barrier 在具体场景进行写回时更好的选择。
static bool table_is_inited = false;
struct mehcached_table table_o;

bool 
get_table_init_state()
{
    memory_barrier();
    return table_is_inited;
}

void 
set_table_init_state(bool val)
{
    memory_barrier();
    table_is_inited = val;
}

static
void
mehcached_print_bucket(const struct mehcached_bucket *bucket)
{
    INFO_LOG("<bucket %zx>\n", (size_t)bucket);
    size_t item_index;
    for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
        INFO_LOG("  item_vec[%zu]: tag=%lu, alloc_id=%lu, item_offset=%lu\n", item_index, MEHCACHED_TAG(bucket->item_vec[item_index]), MEHCACHED_ALLOC_ID(bucket->item_vec[item_index]), MEHCACHED_ITEM_OFFSET(bucket->item_vec[item_index]));
}

static
void
mehcached_print_buckets(const struct mehcached_table *table)
{
    size_t bucket_index;
    for (bucket_index = 0; bucket_index < table->num_buckets; bucket_index++)
    {
        struct mehcached_bucket *bucket = table->buckets + bucket_index;
        mehcached_print_bucket(bucket);
    }
    INFO_LOG("\n");
}

static
void
mehcached_print_stats(const struct mehcached_table *table MEHCACHED_UNUSED)
{
#ifdef MEHCACHED_COLLECT_STATS
    INFO_LOG("count:                  %10zu\n", table->stats.count);
    INFO_LOG("set_nooverwrite:        %10zu | ", table->stats.set_nooverwrite);
    INFO_LOG("set_new:                %10zu | ", table->stats.set_new);
    INFO_LOG("set_inplace:            %10zu | ", table->stats.set_inplace);
    INFO_LOG("set_evicted:            %10zu\n", table->stats.set_evicted);
    INFO_LOG("get_found:              %10zu | ", table->stats.get_found);
    INFO_LOG("get_notfound:           %10zu\n", table->stats.get_notfound);
    INFO_LOG("test_found:             %10zu | ", table->stats.test_found);
    INFO_LOG("test_notfound:          %10zu\n", table->stats.test_notfound);
    INFO_LOG("cleanup:                %10zu\n", table->stats.cleanup);
    INFO_LOG("move_to_head_performed: %10zu | ", table->stats.move_to_head_performed);
    INFO_LOG("move_to_head_skipped:   %10zu | ", table->stats.move_to_head_skipped);
    INFO_LOG("move_to_head_failed:    %10zu\n", table->stats.move_to_head_failed);
#endif
}

static
void
mehcached_reset_table_stats(struct mehcached_table *table MEHCACHED_UNUSED)
{
#ifdef MEHCACHED_COLLECT_STATS
    size_t count = table->stats.count;
    memset(&table->stats, 0, sizeof(table->stats));
    table->stats.count = count;
#endif
}

/* 计算哪一个 key 属于哪一个 value
 * key_hash 取高16位和 bucket 的数量进行取模
 */
static
uint32_t
mehcached_calc_bucket_index(const struct mehcached_table *table, uint64_t key_hash)
{
    // 16 is from MEHCACHED_TAG_MASK's log length
    return (uint32_t)(key_hash >> 16) & table->num_buckets_mask;
}

static
uint16_t
mehcached_calc_tag(uint64_t key_hash)
{
    uint16_t tag = (uint16_t)(key_hash & MEHCACHED_TAG_MASK);
    if (tag == 0)
        return 1;
    else
        return tag;
}

static
// uint64_t
uint32_t
mehcached_read_version_begin(const struct mehcached_table *table MEHCACHED_UNUSED, const struct mehcached_bucket *bucket MEHCACHED_UNUSED)
{
#ifdef MEHCACHED_CONCURRENT
    if (table->concurrent_access_mode != 0)
    {
        while (true)
        {
            // uint64_t v = *(volatile uint64_t *)&bucket->version;
            uint32_t v = *(volatile uint32_t *)&bucket->version;
            memory_barrier();
            // if ((v & 1UL) != 0UL)
            if ((v & 1U) != 0U)
                continue;
            return v;
        }
    }
    else
        // return 0UL;
        return 0U;
#else
    // return 0UL;
    return 0U;
#endif
}

static
//uint64_t
uint32_t
mehcached_read_version_end(const struct mehcached_table *table MEHCACHED_UNUSED, const struct mehcached_bucket *bucket MEHCACHED_UNUSED)
{
#ifdef MEHCACHED_CONCURRENT
    if (table->concurrent_access_mode != 0)
    {
        memory_barrier();
        // uint64_t v = *(volatile uint64_t *)&bucket->version;
        uint32_t v = *(volatile uint32_t *)&bucket->version;
        return v;
    }
    else
        // return 0UL;
        return 0U;
#else
    // return 0UL;
    return 0U;
#endif
}

static
void
mehcached_lock_bucket(const struct mehcached_table *table MEHCACHED_UNUSED, struct mehcached_bucket *bucket MEHCACHED_UNUSED)
{
#ifdef MEHCACHED_CONCURRENT
    if (table->concurrent_access_mode == 1)
    {
        // Assert((*(volatile uint64_t *)&bucket->version & 1UL) == 0UL);
        // (*(volatile uint64_t *)&bucket->version)++;
        Assert((*(volatile uint32_t *)&bucket->version & 1U) == 0U);
        (*(volatile uint32_t *)&bucket->version)++;
        memory_barrier();
    }
    else if (table->concurrent_access_mode == 2)
    {
        while (1)
        {
            // uint64_t v = *(volatile uint64_t *)&bucket->version & ~1UL;
            uint32_t v = *(volatile uint32_t *)&bucket->version & ~1U;
            // uint64_t new_v = v | 1UL;
            uint32_t new_v = v | 1U;
            // if (__sync_bool_compare_and_swap((volatile uint64_t *)&bucket->version, v, new_v))
            if (__sync_bool_compare_and_swap((volatile uint32_t *)&bucket->version, v, new_v))
                break;
        }
    }
#endif
}

static
void
mehcached_unlock_bucket(const struct mehcached_table *table MEHCACHED_UNUSED, struct mehcached_bucket *bucket MEHCACHED_UNUSED)
{
#ifdef MEHCACHED_CONCURRENT
    if (table->concurrent_access_mode != 0)
    {
        memory_barrier();
        // Assert((*(volatile uint64_t *)&bucket->version & 1UL) == 1UL);
        Assert((*(volatile uint32_t *)&bucket->version & 1U) == 1U);
        // no need to use atomic add because this thread is the only one writing to version
        // (*(volatile uint64_t *)&bucket->version)++;
        (*(volatile uint32_t *)&bucket->version)++;
    }
#endif
}

static
void
mehcached_lock_extra_bucket_free_list(struct mehcached_table *table)
{
#ifdef MEHCACHED_CONCURRENT
    if (table->concurrent_access_mode == 2)
    {
        while (1)
        {
            if (__sync_bool_compare_and_swap((volatile uint32_t *)&table->extra_bucket_free_list.lock, 0U, 1U))
                break;
        }
    }
#endif
}

static
void
mehcached_unlock_extra_bucket_free_list(struct mehcached_table *table)
{
#ifdef MEHCACHED_CONCURRENT
    if (table->concurrent_access_mode == 2)
    {
        memory_barrier();
        Assert((*(volatile uint32_t *)&table->extra_bucket_free_list.lock & 1U) == 1U);
        // no need to use atomic add because this thread is the only one writing to version
        *(volatile uint32_t *)&table->extra_bucket_free_list.lock = 0U;
    }
#endif
}

static
bool
mehcached_has_extra_bucket(struct mehcached_bucket *bucket MEHCACHED_UNUSED)
{
#ifndef MEHCACHED_NO_EVICTION
    return false;
#else
    return bucket->next_extra_bucket_index != 0;
#endif
}

static
struct mehcached_bucket *
mehcached_extra_bucket(const struct mehcached_table *table, uint32_t extra_bucket_index)
{
    // extra_bucket_index is 1-base
    Assert(extra_bucket_index != 0);
    Assert(extra_bucket_index < 1 + table->num_extra_buckets);
    return table->extra_buckets + extra_bucket_index;   // extra_buckets[1] is the actual start
}

static
bool
mehcached_alloc_extra_bucket(struct mehcached_table *table, struct mehcached_bucket *bucket)
{
    Assert(!mehcached_has_extra_bucket(bucket));

    mehcached_lock_extra_bucket_free_list(table);

    if (table->extra_bucket_free_list.head == 0)
    {
        mehcached_unlock_extra_bucket_free_list(table);
        return false;
    }

    // take the first free extra bucket
    uint32_t extra_bucket_index = table->extra_bucket_free_list.head;
    table->extra_bucket_free_list.head = mehcached_extra_bucket(table, extra_bucket_index)->next_extra_bucket_index;
    mehcached_extra_bucket(table, extra_bucket_index)->next_extra_bucket_index = 0;

    // add it to the given bucket
    // concurrent readers may see the new extra_bucket from this point
    bucket->next_extra_bucket_index = extra_bucket_index;

    mehcached_unlock_extra_bucket_free_list(table);
    return true;
}

static
void
mehcached_free_extra_bucket(struct mehcached_table *table, struct mehcached_bucket *bucket)
{
    Assert(mehcached_has_extra_bucket(bucket));

    uint32_t extra_bucket_index = bucket->next_extra_bucket_index;

    struct mehcached_bucket *extra_bucket = mehcached_extra_bucket(table, extra_bucket_index);
    Assert(extra_bucket->next_extra_bucket_index == 0); // only allows freeing the tail of the extra bucket chain

    // verify if the extra bucket is empty (debug only)
    size_t item_index;
    for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
        Assert(extra_bucket->item_vec[item_index] == 0);

    // detach
    bucket->next_extra_bucket_index = 0;

    // add freed extra bucket to the free list
    mehcached_lock_extra_bucket_free_list(table);

    extra_bucket->next_extra_bucket_index = table->extra_bucket_free_list.head;
    table->extra_bucket_free_list.head = extra_bucket_index;

    mehcached_unlock_extra_bucket_free_list(table);
}

static
void
mehcached_fill_hole(struct mehcached_table *table, struct mehcached_bucket *bucket, size_t unused_item_index)
{
    // there is no extra bucket; do not try to fill a hole within the same bucket
    if (!mehcached_has_extra_bucket(bucket))
        return;

    struct mehcached_bucket *prev_extra_bucket = NULL;
    struct mehcached_bucket *current_extra_bucket = bucket;
    while (mehcached_has_extra_bucket(current_extra_bucket) != 0)
    {
        prev_extra_bucket = current_extra_bucket;
        current_extra_bucket = mehcached_extra_bucket(table, current_extra_bucket->next_extra_bucket_index);
    }

    bool last_item = true;
    size_t moved_item_index;

    size_t item_index;
    for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
        if (current_extra_bucket->item_vec[item_index] != 0)
        {
            moved_item_index = item_index;
            break;
        }

    for (item_index++; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
        if (current_extra_bucket->item_vec[item_index] != 0)
        {
            last_item = false;
            break;
        }

    // move the entry
    bucket->item_vec[unused_item_index] = current_extra_bucket->item_vec[moved_item_index];
    current_extra_bucket->item_vec[moved_item_index] = 0;

    // if it was the last entry of current_extra_bucket, free current_extra_bucket
    if (last_item)
        mehcached_free_extra_bucket(table, prev_extra_bucket);
}

static
size_t
mehcached_find_empty(struct mehcached_table *table, struct mehcached_bucket *bucket, struct mehcached_bucket **located_bucket)
{
    struct mehcached_bucket *current_bucket = bucket;
    while (true)
    {
        size_t item_index;
        for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
        {
            if (current_bucket->item_vec[item_index] == 0)
            {
                *located_bucket = current_bucket;
                return item_index;
            }
        }
        if (!mehcached_has_extra_bucket(current_bucket))
            break;
        current_bucket = mehcached_extra_bucket(table, current_bucket->next_extra_bucket_index);
    }

    // no space; alloc new extra_bucket
    if (mehcached_alloc_extra_bucket(table, current_bucket))
    {
        *located_bucket = mehcached_extra_bucket(table, current_bucket->next_extra_bucket_index);
        return 0;   // use the first slot (it should be empty)
    }
    else
    {
        // no free extra_bucket
        *located_bucket = NULL;
        return MEHCACHED_ITEMS_PER_BUCKET;
    }
}

static
size_t
mehcached_find_empty_or_oldest(const struct mehcached_table *table, struct mehcached_bucket *bucket, struct mehcached_bucket **located_bucket)
{
#if defined(MEHCACHED_ALLOC_MALLOC) || defined(MEHCACHED_ALLOC_DYNAMIC)
    // this code should be reachable because we can now use MEHCACHED_NO_EVICTION
    Assert(false);
#endif

#ifdef MEHCACHED_ALLOC_POOL
    size_t oldest_item_index = 0;
    uint64_t oldest_item_distance = (uint64_t)-1;
    struct mehcached_bucket *oldest_item_bucket = NULL;
#endif

    struct mehcached_bucket *current_bucket = bucket;
    while (true)
    {
        size_t item_index;
        for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
        {
            if (current_bucket->item_vec[item_index] == 0)
            {
                *located_bucket = current_bucket;
                return item_index;
            }
#ifdef MEHCACHED_ALLOC_POOL
            uint8_t alloc_id = MEHCACHED_ALLOC_ID(current_bucket->item_vec[item_index]);
            uint64_t distance = (table->alloc[alloc_id].tail - MEHCACHED_ITEM_OFFSET(current_bucket->item_vec[item_index])) & MEHCACHED_ITEM_OFFSET_MASK;
            if (oldest_item_distance > distance)
            {
                oldest_item_distance = distance;
                oldest_item_index = item_index;
                oldest_item_bucket = current_bucket;
            }
#endif
        }
        if (!mehcached_has_extra_bucket(current_bucket))
            break;
        current_bucket = mehcached_extra_bucket(table, current_bucket->next_extra_bucket_index);
    }

#ifdef MEHCACHED_ALLOC_POOL
    *located_bucket = oldest_item_bucket;
    return oldest_item_index;
#endif
#if defined(MEHCACHED_ALLOC_MALLOC) || defined(MEHCACHED_ALLOC_DYNAMIC)
    // XXX: getting oldest item is unclear with malloc; just pick anything
    *located_bucket = current_bucket;
    static size_t v = 0;    // XXX: this is slow with multiple threads!
    size_t item_index = (v++) % MEHCACHED_ITEMS_PER_BUCKET;
    return item_index;
#endif
}

static
void
mehcached_set_item(struct mehcached_item *item, uint64_t key_hash, const uint8_t *key, uint32_t key_length, const uint8_t *value, uint32_t value_length, uint32_t expire_time)
{
    Assert(key_length <= MEHCACHED_KEY_MASK);
    Assert(value_length <= MEHCACHED_VALUE_MASK);

    item->kv_length_vec = MEHCACHED_KV_LENGTH_VEC(key_length, value_length);
    item->key_hash = key_hash;
    item->expire_time = expire_time;
    mehcached_memcpy8(item->data, key, key_length);
    mehcached_memcpy8(item->data + MEHCACHED_ROUNDUP8(key_length), value, value_length);
}

static
void
mehcached_set_item_value(struct mehcached_item *item, const uint8_t *value, uint32_t value_length, uint32_t expire_time)
{
    Assert(value_length <= MEHCACHED_VALUE_MASK);

    uint32_t key_length = MEHCACHED_KEY_LENGTH(item->kv_length_vec);
    item->kv_length_vec = MEHCACHED_KV_LENGTH_VEC(key_length, value_length);
    item->expire_time = expire_time;
    
    /*
     *
     * TODO：
     * 增加RDMA操作，同时需要写远端
     */
    mehcached_memcpy8(item->data + MEHCACHED_ROUNDUP8(key_length), value, value_length);
}

static
bool
mehcached_compare_keys(const uint8_t *key1, size_t key1_len, const uint8_t *key2, size_t key2_len)
{
    return key1_len == key2_len && mehcached_memcmp8(key1, key2, key1_len);
}

static
size_t
mehcached_find_item_index(const struct mehcached_table *table, struct mehcached_bucket *bucket, uint64_t key_hash,\
                 uint16_t tag, const uint8_t *key, size_t key_length, struct mehcached_bucket **located_bucket)
{
    struct mehcached_bucket *current_bucket = bucket;

    while (true)
    {
        size_t item_index;
        for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
        {
            // tag 存在的目的是为了加快相同 bucket 中的对 key 的搜索速度
            if (MEHCACHED_TAG(current_bucket->item_vec[item_index]) != tag)
                continue;

            // we may read garbage values, which do not cause any fatal issue
    #ifdef MEHCACHED_ALLOC_POOL
            uint8_t alloc_id = MEHCACHED_ALLOC_ID(current_bucket->item_vec[item_index]);
            struct mehcached_item *item = (struct mehcached_item *)mehcached_pool_item(&table->alloc[alloc_id], MEHCACHED_ITEM_OFFSET(current_bucket->item_vec[item_index]));
    #endif
    #ifdef MEHCACHED_ALLOC_MALLOC
            struct mehcached_item *item = (struct mehcached_item *)mehcached_malloc_item(&table->alloc, MEHCACHED_ITEM_OFFSET(current_bucket->item_vec[item_index]));
    #endif
    #ifdef MEHCACHED_ALLOC_DYNAMIC
            struct mehcached_item *item = (struct mehcached_item *)mehcached_dynamic_item(&table->alloc, MEHCACHED_ITEM_OFFSET(current_bucket->item_vec[item_index]));
    #endif

            if (item->key_hash != key_hash)
                continue;

            // a key comparison reads up to min(source key length and destination key length), which is always safe to do
            // min(source key length and destination key length)
            if (!mehcached_compare_keys(item->data, MEHCACHED_KEY_LENGTH(item->kv_length_vec), key, key_length))
                continue;

            // we skip any validity check because it will be done by callers who are doing more jobs with this result

    #ifdef MEHCACHED_VERBOSE
            INFO_LOG("find item index: %zu\n", item_index);
    #endif
            *located_bucket = current_bucket;
            return item_index;
        }

        if (!mehcached_has_extra_bucket(current_bucket))
            break;
        current_bucket = mehcached_extra_bucket(table, current_bucket->next_extra_bucket_index);
    }

#ifdef MEHCACHED_VERBOSE
    INFO_LOG("could not find item index\n");
#endif

    *located_bucket = NULL;
    return MEHCACHED_ITEMS_PER_BUCKET;
}

static
size_t
mehcached_find_same_tag(const struct mehcached_table *table, struct mehcached_bucket *bucket, uint16_t tag, struct mehcached_bucket **located_bucket)
{
    struct mehcached_bucket *current_bucket = bucket;

    while (true)
    {
        size_t item_index;
        for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
        {
            if (MEHCACHED_TAG(current_bucket->item_vec[item_index]) != tag)
                continue;

            *located_bucket = current_bucket;
            return item_index;
        }

        if (!mehcached_has_extra_bucket(current_bucket))
            break;
        current_bucket = mehcached_extra_bucket(table, current_bucket->next_extra_bucket_index);
    }

    *located_bucket = NULL;
    return MEHCACHED_ITEMS_PER_BUCKET;
}

#ifdef MEHCACHED_ALLOC_POOL
static
void
mehcached_cleanup_bucket(uint8_t current_alloc_id, struct mehcached_table *table, uint64_t old_tail, uint64_t new_tail)
{
    // XXX: in principal, we should use old/new head for cleanup instead of tails because head changes are what invalidates locations.
    // however, using tails does the same job as using heads by large because tail change >= head change.

    // tails are using 64-bit numbers, but it is OK because we apply table->num_buckets_mask mask
    uint64_t bucket_index = (old_tail >> table->rshift) & table->num_buckets_mask;
    uint64_t bucket_index_end = (new_tail >> table->rshift) & table->num_buckets_mask;

    while (bucket_index != bucket_index_end)
    {
#ifdef MEHCACHED_VERBOSE
        INFO_LOG("cleanup bucket: %lu\n", bucket_index);
#endif
        struct mehcached_bucket *bucket = table->buckets + bucket_index;

        mehcached_lock_bucket(table, bucket);

        struct mehcached_bucket *current_bucket = bucket;
        while (true)
        {
            size_t item_index;
            for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
            {
                uint64_t *item_vec_p = &current_bucket->item_vec[item_index];
                if (*item_vec_p == 0)
                    continue;

                // skip other alloc's items because it will incur access to that allocator
                if (MEHCACHED_ALLOC_ID(*item_vec_p) != current_alloc_id)
                    continue;

                if (!mehcached_pool_is_valid(&table->alloc[current_alloc_id], MEHCACHED_ITEM_OFFSET(*item_vec_p)))
                {
                    *item_vec_p = 0;
                    MEHCACHED_STAT_INC(table, cleanup);
                    MEHCACHED_STAT_DEC(table, count);
                }
            }

            if (!mehcached_has_extra_bucket(current_bucket))
                break;
            current_bucket = mehcached_extra_bucket(table, current_bucket->next_extra_bucket_index);
        }

        mehcached_unlock_bucket(table, bucket);
        bucket_index = (bucket_index + 1UL) & table->num_buckets_mask;
    }
}

static
void
mehcached_cleanup_all(uint8_t current_alloc_id, struct mehcached_table *table)
{
    mehcached_cleanup_bucket(current_alloc_id, table, 0, (uint64_t)1 << table->rshift);
    mehcached_cleanup_bucket(current_alloc_id, table, (uint64_t)1 << table->rshift, 0);
}
#endif

static
void
mehcached_prefetch_table(struct mehcached_table *table, uint64_t key_hash, struct mehcached_prefetch_state *out_prefetch_state)
{
    struct mehcached_prefetch_state *prefetch_state = out_prefetch_state;

    uint32_t bucket_index = mehcached_calc_bucket_index(table, key_hash);

    prefetch_state->table = table;
    prefetch_state->bucket = table->buckets + bucket_index;
    prefetch_state->key_hash = key_hash;

    // bucket address is already 64-byte aligned
    __builtin_prefetch(prefetch_state->bucket, 0, 0);

    if (MEHCACHED_ITEMS_PER_BUCKET > 7)
        __builtin_prefetch((uint8_t *)prefetch_state->bucket + 64, 0, 0);

    // XXX: prefetch extra buckets, too?
}

static
void
mehcached_prefetch_alloc(struct mehcached_prefetch_state *in_out_prefetch_state)
{
    struct mehcached_prefetch_state *prefetch_state = in_out_prefetch_state;

    struct mehcached_table *table = prefetch_state->table;
    struct mehcached_bucket *bucket = prefetch_state->bucket;
    uint16_t tag = mehcached_calc_tag(prefetch_state->key_hash);

    size_t item_index;
    for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
    {
        if (MEHCACHED_TAG(bucket->item_vec[item_index]) != tag)
            continue;

#ifdef MEHCACHED_ALLOC_POOL
        uint8_t alloc_id = MEHCACHED_ALLOC_ID(bucket->item_vec[item_index]);
        struct mehcached_item *item = (struct mehcached_item *)mehcached_pool_item(&table->alloc[alloc_id], MEHCACHED_ITEM_OFFSET(bucket->item_vec[item_index]));
#endif
#ifdef MEHCACHED_ALLOC_MALLOC
        struct mehcached_item *item = (struct mehcached_item *)mehcached_malloc_item(&table->alloc, MEHCACHED_ITEM_OFFSET(bucket->item_vec[item_index]));
#endif
#ifdef MEHCACHED_ALLOC_DYNAMIC
        struct mehcached_item *item = (struct mehcached_item *)mehcached_dynamic_item(&table->alloc, MEHCACHED_ITEM_OFFSET(bucket->item_vec[item_index]));
#endif

        // prefetch the item's cache line and the subsequence cache line
        __builtin_prefetch((void *)(((size_t)item & ~(size_t)63) + 0), 0, 0);
        __builtin_prefetch((void *)(((size_t)item & ~(size_t)63) + 64), 0, 0);
    }
}

static
bool
mehcached_get(uint8_t current_alloc_id MEHCACHED_UNUSED, struct mehcached_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length, uint8_t *out_value, size_t *in_out_value_length, uint32_t *out_expire_time, bool readonly MEHCACHED_UNUSED)
{
    Assert(key_length <= MEHCACHED_MAX_KEY_LENGTH);

    uint32_t bucket_index = mehcached_calc_bucket_index(table, key_hash);
    uint16_t tag = mehcached_calc_tag(key_hash);

    struct mehcached_bucket *bucket = table->buckets + bucket_index;

    bool partial_value;
    uint32_t expire_time;

    while (true)
    {
        uint32_t version_start = mehcached_read_version_begin(table, bucket);

        struct mehcached_bucket *located_bucket;
        size_t item_index = mehcached_find_item_index(table, bucket, key_hash, tag, key, key_length, &located_bucket);
        if (item_index == MEHCACHED_ITEMS_PER_BUCKET)
        {
            if (version_start != mehcached_read_version_end(table, bucket))
                continue;
            MEHCACHED_STAT_INC(table, get_notfound);
            return false;
        }

        uint64_t item_vec = located_bucket->item_vec[item_index];
        uint64_t item_offset = MEHCACHED_ITEM_OFFSET(item_vec);

#ifdef MEHCACHED_ALLOC_POOL
        // we may read garbage data, but all operations relying on them are safe here
        uint64_t alloc_id = MEHCACHED_ALLOC_ID(item_vec);
        if (alloc_id >= (uint64_t)table->alloc_id_mask + 1)   // fix-up for possible garbage read
            alloc_id = 0;
        struct mehcached_item *item = (struct mehcached_item *)mehcached_pool_item(&table->alloc[alloc_id], item_offset);
#endif
#ifdef MEHCACHED_ALLOC_MALLOC
        struct mehcached_item *item = (struct mehcached_item *)mehcached_malloc_item(&table->alloc, item_offset);
#endif
#ifdef MEHCACHED_ALLOC_DYNAMIC
        struct mehcached_item *item = (struct mehcached_item *)mehcached_dynamic_item(&table->alloc, item_offset);
#endif

        expire_time = item->expire_time;

        size_t key_length = MEHCACHED_KEY_LENGTH(item->kv_length_vec);
        if (key_length > MEHCACHED_MAX_KEY_LENGTH)
            key_length = MEHCACHED_MAX_KEY_LENGTH;  // fix-up for possible garbage read

        size_t value_length = MEHCACHED_VALUE_LENGTH(item->kv_length_vec);
        if (value_length > MEHCACHED_MAX_VALUE_LENGTH)
            value_length = MEHCACHED_MAX_VALUE_LENGTH;  // fix-up for possible garbage read

#ifndef NDEBUG
        // debug code to check how the code works when it read garbage values
        // if all inserted values are no larger than 1500 and this debug message is printed, something bad is occurring
        if (value_length > 1500)
        {
#ifdef MEHCACHED_ALLOC_POOL
            fprintf(stderr, "head %lu\n", table->alloc[alloc_id].head);
            fprintf(stderr, "tail %lu\n", table->alloc[alloc_id].tail);
#endif
            fprintf(stderr, "item_offset %lu\n", item_offset);
        }
#endif

#ifdef MEHCACHED_ALLOC_POOL
        // for move-to-head
        // this does not use item->alloc_item.item_size to discard currently unused space within the item
        uint32_t item_size = (uint32_t)(sizeof(struct mehcached_item) + MEHCACHED_ROUNDUP8(key_length) + MEHCACHED_ROUNDUP8(value_length));
#endif

        // adjust value length to use
        if (value_length > *in_out_value_length)
        {
            partial_value = true;
            value_length = *in_out_value_length;
        }
        else
            partial_value = false;
        mehcached_memcpy8(out_value, item->data + MEHCACHED_ROUNDUP8(key_length), value_length);

#ifdef MEHCACHED_ALLOC_POOL
        if (!mehcached_pool_is_valid(&table->alloc[alloc_id], item_offset))
        {
            if (version_start != mehcached_read_version_end(table, bucket))
                continue;

            if (!readonly)
            {
                // remove stale item; this may remove some wrong item, but we do not care because
                // (1) if this key has been deleted and reinserted, it is none of our business here
                // (2) if this key has been deleted and a different key was inserted, we delete innocent key, but without any fatal issue.
                // this will slow down query speed for outdated matching key at first, but improves it later by skipping the value copy step
                mehcached_lock_bucket(table, bucket);
                if (located_bucket->item_vec[item_index] != 0)
                {
                    located_bucket->item_vec[item_index] = 0;
                    MEHCACHED_STAT_DEC(table, count);
                }
                mehcached_unlock_bucket(table, bucket);
            }

            MEHCACHED_STAT_INC(table, get_notfound);
            return false;
        }
#endif

        if (version_start != mehcached_read_version_end(table, bucket))
            continue;

#ifndef NDEBUG
        // debug code to check how the code works when it read garbage values
        if (value_length > 1500)
            Assert(false);
#endif

        *in_out_value_length = value_length;
        if (out_expire_time != NULL)
            *out_expire_time = expire_time;

        // the following is optional processing (we will return the value retrieved above)

        MEHCACHED_STAT_INC(table, get_found);

#ifdef MEHCACHED_ALLOC_POOL
        if (!readonly && alloc_id == current_alloc_id)
        {
            // small distance_from_tail = recently appended at tail
            // it is not required to acquire a lock to read table->alloc.tail because
            // some inaccurate number is OK
            // note that we are accessing table->alloc.tail, which uses a 64-bit number; this is OK because we apply table->alloc.mask mask
            // perform move-to-head within the same alloc only

            uint64_t distance_from_tail = (table->alloc[current_alloc_id].tail - item_offset) & table->alloc[current_alloc_id].mask;
            if (distance_from_tail > table->mth_threshold)
            {
                mehcached_lock_bucket(table, bucket);
                mehcached_pool_lock(&table->alloc[current_alloc_id]);

                // check if the original item is still there
                if (located_bucket->item_vec[item_index] == item_vec)
                {
                    // allocate new space
                    uint64_t new_item_offset = mehcached_pool_allocate(&table->alloc[current_alloc_id], item_size);
                    uint64_t new_tail = table->alloc[current_alloc_id].tail;

                    // see if the original item is still valid because mehcached_pool_allocate() by this thread or other threads may have invalidated it
                    if (mehcached_pool_is_valid(&table->alloc[current_alloc_id], item_offset))
                    {
                        struct mehcached_item *new_item = (struct mehcached_item *)mehcached_pool_item(&table->alloc[current_alloc_id], new_item_offset);
                        mehcached_memcpy8((uint8_t *)new_item, (const uint8_t *)item, item_size);

                        uint64_t new_item_vec = MEHCACHED_ITEM_VEC(MEHCACHED_TAG(item_vec), current_alloc_id, new_item_offset);

                        located_bucket->item_vec[item_index] = new_item_vec;

                        // success
                        MEHCACHED_STAT_INC(table, move_to_head_performed);
                    }
                    else
                    {
                        // failed -- original data become invalid in the alloc
                        MEHCACHED_STAT_INC(table, move_to_head_failed);
                    }

                    // we need to hold the lock until we finish writing
                    mehcached_pool_unlock(&table->alloc[current_alloc_id]);
                    mehcached_unlock_bucket(table, bucket);

                    // XXX: this may be too late; before cleaning, other threads may have read some invalid location
                    //      ideally, this should be done before writing actual data
                    mehcached_cleanup_bucket(current_alloc_id, table, new_item_offset, new_tail);
                }
                else
                {
                    mehcached_pool_unlock(&table->alloc[current_alloc_id]);
                    mehcached_unlock_bucket(table, bucket);

                    // failed -- original data become invalid in the table
                    MEHCACHED_STAT_INC(table, move_to_head_failed);
                }
            }
            else
            {
                MEHCACHED_STAT_INC(table, move_to_head_skipped);
            }
        }
#endif
        break;
    }

    // TODO: check partial_value and return a correct code
    (void)partial_value;

#ifndef NDEBUG
    // debug code to check how the code works when it read garbage values
    if (*in_out_value_length > 1500)
        Assert(false);
#endif

    return true;
}

static
bool
mehcached_test(uint8_t current_alloc_id MEHCACHED_UNUSED, struct mehcached_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length)
{
    Assert(key_length <= MEHCACHED_MAX_KEY_LENGTH);

    uint32_t bucket_index = mehcached_calc_bucket_index(table, key_hash);
    uint16_t tag = mehcached_calc_tag(key_hash);

    struct mehcached_bucket *bucket = table->buckets + bucket_index;

    while (true)
    {
        uint32_t version_start = mehcached_read_version_begin(table, bucket);

        struct mehcached_bucket *located_bucket;
        size_t item_index = mehcached_find_item_index(table, bucket, key_hash, tag, key, key_length, &located_bucket);

        if (version_start != mehcached_read_version_end(table, bucket))
            continue;

        if (item_index != MEHCACHED_ITEMS_PER_BUCKET)
        {
            MEHCACHED_STAT_INC(table, test_found);
            return true;
        }
        else
        {
            MEHCACHED_STAT_INC(table, test_notfound);
            return false;
        }
    }

    // not reachable
    return false;
}

static
bool
mehcached_set(uint8_t current_alloc_id, struct mehcached_table *table, uint64_t key_hash,\
                const uint8_t *key, size_t key_length, const uint8_t *value, size_t value_length,\
                uint32_t expire_time, bool overwrite)
{
    Assert(key_length <= MEHCACHED_MAX_KEY_LENGTH);
    Assert(value_length <= MEHCACHED_MAX_VALUE_LENGTH);

    uint32_t bucket_index = mehcached_calc_bucket_index(table, key_hash);
    /*
     * 每个索引条目都包含项目的部分信息： tag 和日志中的 item offset， tag 是索引项的密钥散列的另一部分，用于过滤不匹配的查找键：
     * 它可以通过比较存储的 tag 和查找密钥散列中的 tag 来判断索引项是否永远不会与查找键匹配。
     * 我们通过将其设为 1 来避免使用零 tag ，因为我们使用零值来指示空索引条目。 通过向索引条目写入零值来删除项目； 日志中的条目将被自动垃圾收集。
     */
    uint16_t tag = mehcached_calc_tag(key_hash);

    struct mehcached_bucket *bucket = table->buckets + bucket_index;

    bool overwriting;

    mehcached_lock_bucket(table, bucket);

    struct mehcached_bucket *located_bucket;
    size_t item_index = mehcached_find_item_index(table, bucket, key_hash, tag, key, key_length, &located_bucket);

    // 当前被插入的 key 以及存在于 hash 表中
    if (item_index != MEHCACHED_ITEMS_PER_BUCKET)
    {
        if (!overwrite)
        {
            MEHCACHED_STAT_INC(table, set_nooverwrite);

            mehcached_unlock_bucket(table, bucket);
            return false;   // already exist but cannot overwrite
        }
        else
        {
            overwriting = true;
        }
    }
    else
    {
#ifndef MEHCACHED_NO_EVICTION
        // pick an item with the same tag; this is potentially the same item but invalid due to insufficient log space
        // this helps limiting the effect of "ghost" items in the table when the log space is slightly smaller than enough
        // and makes better use of the log space by not evicting unrelated old items in the same bucket
        // 总结来说，就是寻找相同 tag 的 item， 因为可能之前有相同的 tag 以及失效了（因为大小不满足），所以我们可以继续尝试利用这些相同大小的 tag 
        item_index = mehcached_find_same_tag(table, bucket, tag, &located_bucket);

        if (item_index == MEHCACHED_ITEMS_PER_BUCKET)
        {
            // if there is no matching tag, we just use the empty or oldest item
            item_index = mehcached_find_empty_or_oldest(table, bucket, &located_bucket);
        }
#else
        item_index = mehcached_find_empty(table, bucket, &located_bucket);
        if (item_index == MEHCACHED_ITEMS_PER_BUCKET)
        {
            // no more space
            // TODO: add a statistics entry
            mehcached_unlock_bucket(table, bucket);
            return false;
        }
#endif

        overwriting = false;
    }

    uint32_t new_item_size = (uint32_t)(sizeof(struct mehcached_item) + MEHCACHED_ROUNDUP8(key_length) + MEHCACHED_ROUNDUP8(value_length));
    uint64_t item_offset;

#ifdef MEHCACHED_ALLOC_POOL
    // we have to lock the pool because is_valid check must be correct in the overwrite mode;
    // unlike reading, we cannot afford writing data at a wrong place
    mehcached_pool_lock(&table->alloc[current_alloc_id]);
#endif

    if (overwriting)
    {
        item_offset = MEHCACHED_ITEM_OFFSET(located_bucket->item_vec[item_index]);

        if (MEHCACHED_ALLOC_ID(located_bucket->item_vec[item_index]) == current_alloc_id)
        {
            // do in-place update only when alloc_id matches to avoid write threshing

#ifdef MEHCACHED_ALLOC_POOL
            struct mehcached_item *item = (struct mehcached_item *)mehcached_pool_item(&table->alloc[current_alloc_id], item_offset);
#endif
#ifdef MEHCACHED_ALLOC_MALLOC
            struct mehcached_item *item = (struct mehcached_item *)mehcached_malloc_item(&table->alloc, item_offset);
#endif
#ifdef MEHCACHED_ALLOC_DYNAMIC
            struct mehcached_item *item = (struct mehcached_item *)mehcached_dynamic_item(&table->alloc, item_offset);
#endif

#ifdef MEHCACHED_ALLOC_POOL
            if (mehcached_pool_is_valid(&table->alloc[current_alloc_id], item_offset))
#endif
            {
                if (item->alloc_item.item_size >= new_item_size)
                {
                    MEHCACHED_STAT_INC(table, set_inplace);

                    mehcached_set_item_value(item, value, (uint32_t)value_length, expire_time);

#ifdef MEHCACHED_ALLOC_POOL
                    mehcached_pool_unlock(&table->alloc[current_alloc_id]);
#endif
                    mehcached_unlock_bucket(table, bucket);
                    return true;
                }
            }
        }
    }

#ifdef MEHCACHED_ALLOC_POOL
    uint64_t new_item_offset = mehcached_pool_allocate(&table->alloc[current_alloc_id], new_item_size);
    uint64_t new_tail = table->alloc[current_alloc_id].tail;
    struct mehcached_item *new_item = (struct mehcached_item *)mehcached_pool_item(&table->alloc[current_alloc_id], new_item_offset);
#endif
#ifdef MEHCACHED_ALLOC_MALLOC
    uint64_t new_item_offset = mehcached_malloc_allocate(&table->alloc, new_item_size);
    if (new_item_offset == MEHCACHED_MALLOC_INSUFFICIENT_SPACE)
    {
        // no more space
        // TODO: add a statistics entry
        mehcached_unlock_bucket(table, bucket);
        return false;
    }
    struct mehcached_item *new_item = (struct mehcached_item *)mehcached_malloc_item(&table->alloc, new_item_offset);
#endif
#ifdef MEHCACHED_ALLOC_DYNAMIC
    mehcached_dynamic_lock(&table->alloc);
    uint64_t new_item_offset = mehcached_dynamic_allocate(&table->alloc, new_item_size);
    mehcached_dynamic_unlock(&table->alloc);
    if (new_item_offset == MEHCACHED_DYNAMIC_INSUFFICIENT_SPACE)
    {
        // no more space
        // TODO: add a statistics entry
        mehcached_unlock_bucket(table, bucket);
        return false;
    }
    struct mehcached_item *new_item = (struct mehcached_item *)mehcached_dynamic_item(&table->alloc, new_item_offset);
#endif

    MEHCACHED_STAT_INC(table, set_new);

    mehcached_set_item(new_item, key_hash, key, (uint32_t)key_length, value, (uint32_t)value_length, expire_time);
#ifdef MEHCACHED_ALLOC_POOL
    // unlocking is delayed until we finish writing data at the new location;
    // otherwise, the new location may be invalidated (in a very rare case)
    mehcached_pool_unlock(&table->alloc[current_alloc_id]);
#endif

    if (located_bucket->item_vec[item_index] != 0)
    {
        MEHCACHED_STAT_INC(table, set_evicted);
        MEHCACHED_STAT_DEC(table, count);
    }

    located_bucket->item_vec[item_index] = MEHCACHED_ITEM_VEC(tag, current_alloc_id, new_item_offset);

    mehcached_unlock_bucket(table, bucket);

#ifdef MEHCACHED_ALLOC_POOL
    // XXX: this may be too late; before cleaning, other threads may have read some invalid location
    //      ideally, this should be done before writing actual data
    mehcached_cleanup_bucket(current_alloc_id, table, new_item_offset, new_tail);
#endif

#ifdef MEHCACHED_ALLOC_MALLOC
    // this is done after bucket is updated and unlocked
    if (overwriting)
        mehcached_malloc_deallocate(&table->alloc, item_offset);
#endif

#ifdef MEHCACHED_ALLOC_DYNAMIC
    // this is done after bucket is updated and unlocked
    if (overwriting)
    {
        mehcached_dynamic_lock(&table->alloc);
        mehcached_dynamic_deallocate(&table->alloc, item_offset);
        mehcached_dynamic_unlock(&table->alloc);
    }
#endif

    MEHCACHED_STAT_INC(table, count);

    return true;
}

static
bool
mehcached_delete(uint8_t alloc_id MEHCACHED_UNUSED, struct mehcached_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length)
{
    Assert(key_length <= MEHCACHED_MAX_KEY_LENGTH);

    uint32_t bucket_index = mehcached_calc_bucket_index(table, key_hash);
    uint16_t tag = mehcached_calc_tag(key_hash);

    struct mehcached_bucket *bucket = table->buckets + bucket_index;

    mehcached_lock_bucket(table, bucket);

    struct mehcached_bucket *located_bucket;
    size_t item_index = mehcached_find_item_index(table, bucket, key_hash, tag, key, key_length, &located_bucket);
    if (item_index == MEHCACHED_ITEMS_PER_BUCKET)
    {
        mehcached_unlock_bucket(table, bucket);
        MEHCACHED_STAT_INC(table, delete_notfound);
        return false;
    }

    located_bucket->item_vec[item_index] = 0;
    MEHCACHED_STAT_DEC(table, count);

    mehcached_fill_hole(table, located_bucket, item_index);

    mehcached_unlock_bucket(table, bucket);

    MEHCACHED_STAT_INC(table, delete_found);
    return true;
}

static
bool
mehcached_increment(uint8_t current_alloc_id, struct mehcached_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length, uint64_t increment, uint64_t *out_new_value, uint32_t expire_time)
{
    Assert(key_length <= MEHCACHED_MAX_KEY_LENGTH);

    uint32_t bucket_index = mehcached_calc_bucket_index(table, key_hash);
    uint16_t tag = mehcached_calc_tag(key_hash);

    struct mehcached_bucket *bucket = table->buckets + bucket_index;

    // TODO: add stats

    mehcached_lock_bucket(table, bucket);

    struct mehcached_bucket *located_bucket;
    size_t item_index = mehcached_find_item_index(table, bucket, key_hash, tag, key, key_length, &located_bucket);

    if (item_index == MEHCACHED_ITEMS_PER_BUCKET)
    {
        mehcached_unlock_bucket(table, bucket);
        // TODO: support seeding a new item with the given default value?
        return false;   // does not exist
    }

    if (current_alloc_id != MEHCACHED_ALLOC_ID(located_bucket->item_vec[item_index]))
    {
        mehcached_unlock_bucket(table, bucket);
        return false;   // writing to a different alloc is not allowed
    }

    uint64_t item_offset = MEHCACHED_ITEM_OFFSET(located_bucket->item_vec[item_index]);

#ifdef MEHCACHED_ALLOC_POOL
    // ensure that the item is still valid
    mehcached_pool_lock(&table->alloc[current_alloc_id]);

    if (!mehcached_pool_is_valid(&table->alloc[current_alloc_id], item_offset))
    {
        mehcached_pool_unlock(&table->alloc[current_alloc_id]);
        mehcached_unlock_bucket(table, bucket);
        // TODO: support seeding a new item with the given default value?
        return false;   // exists in the table but not valid in the pool
    }
#endif

#ifdef MEHCACHED_ALLOC_POOL
    struct mehcached_item *item = (struct mehcached_item *)mehcached_pool_item(&table->alloc[current_alloc_id], item_offset);
#endif
#ifdef MEHCACHED_ALLOC_MALLOC
    struct mehcached_item *item = (struct mehcached_item *)mehcached_malloc_item(&table->alloc, item_offset);
#endif
#ifdef MEHCACHED_ALLOC_DYNAMIC
    struct mehcached_item *item = (struct mehcached_item *)mehcached_dynamic_item(&table->alloc, item_offset);
#endif

    size_t value_length = MEHCACHED_VALUE_LENGTH(item->kv_length_vec);
    if (value_length != sizeof(uint64_t))
    {
#ifdef MEHCACHED_ALLOC_POOL
        mehcached_pool_unlock(&table->alloc[current_alloc_id]);
#endif
        mehcached_unlock_bucket(table, bucket);
        return false;   // invalid value size
    }

    uint64_t old_value;
    mehcached_memcpy8((uint8_t *)&old_value, item->data + MEHCACHED_ROUNDUP8(key_length), sizeof(old_value));

    uint64_t new_value = old_value + increment;
    mehcached_memcpy8(item->data + MEHCACHED_ROUNDUP8(key_length), (const uint8_t *)&new_value, sizeof(new_value));
    item->expire_time = expire_time;

    *out_new_value = new_value;

#ifdef MEHCACHED_ALLOC_POOL
    mehcached_pool_unlock(&table->alloc[current_alloc_id]);
#endif
    mehcached_unlock_bucket(table, bucket);

    return true;
}

static
void
mehcached_table_reset(struct mehcached_table *table)
{
    size_t bucket_index;
#ifdef MEHCACHED_ALLOC_DYNAMIC
    mehcached_dynamic_lock(&table->alloc);
#endif
    for (bucket_index = 0; bucket_index < table->num_buckets; bucket_index++)
    {
        struct mehcached_bucket *bucket = table->buckets + bucket_index;
        size_t item_index;
        for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
            if (bucket->item_vec[item_index] != 0)
            {
#ifdef MEHCACHED_ALLOC_MALLOC
                mehcached_malloc_deallocate(&table->alloc, MEHCACHED_ITEM_OFFSET(bucket->item_vec[item_index]));
#endif
#ifdef MEHCACHED_ALLOC_DYNAMIC
                mehcached_dynamic_deallocate(&table->alloc, MEHCACHED_ITEM_OFFSET(bucket->item_vec[item_index]));
#endif
            }
    }
#ifdef MEHCACHED_ALLOC_DYNAMIC
    mehcached_dynamic_unlock(&table->alloc);
#endif

    memset(table->buckets, 0, sizeof(struct mehcached_bucket) * (table->num_buckets + table->num_extra_buckets));

    // initialize a free list of extra buckets
    if (table->num_extra_buckets == 0)
        table->extra_bucket_free_list.head = 0;    // no extra bucket at all
    else
    {
        uint32_t extra_bucket_index;
        for (extra_bucket_index = 1; extra_bucket_index < 1 + table->num_extra_buckets - 1; extra_bucket_index++)
            (table->extra_buckets + extra_bucket_index)->next_extra_bucket_index = extra_bucket_index + 1;
        (table->extra_buckets + extra_bucket_index)->next_extra_bucket_index = 0;    // no more free extra bucket

        table->extra_bucket_free_list.head = 1;
    }

#ifdef MEHCACHED_ALLOC_POOL
    size_t alloc_id;
    for (alloc_id = 0; alloc_id < (size_t)(table->alloc_id_mask + 1); alloc_id++)
    {
        mehcached_pool_lock(&table->alloc[alloc_id]);
        mehcached_pool_reset(&table->alloc[alloc_id]);
        mehcached_pool_unlock(&table->alloc[alloc_id]);
    }
#endif
#ifdef MEHCACHED_ALLOC_MALLOC
    mehcached_malloc_reset(&table->alloc);
#endif
#ifdef MEHCACHED_ALLOC_DYNAMIC
    mehcached_dynamic_reset(&table->alloc);
#endif

#ifdef MEHCACHED_COLLECT_STATS
    table->stats.count = 0;
#endif
    mehcached_reset_table_stats(table);
}

void
mehcached_table_init(struct mehcached_table *table, size_t num_buckets, size_t num_pools MEHCACHED_UNUSED,\
                    size_t pool_size MEHCACHED_UNUSED, bool concurrent_table_read, bool concurrent_table_write,\
                    bool concurrent_alloc_write, size_t table_numa_node, size_t alloc_numa_nodes[], double mth_threshold)
{
    Assert((MEHCACHED_ITEMS_PER_BUCKET == 7 && sizeof(struct mehcached_bucket) == 64) || (MEHCACHED_ITEMS_PER_BUCKET == 15 && sizeof(struct mehcached_bucket) == 128) || (MEHCACHED_ITEMS_PER_BUCKET == 31 && sizeof(struct mehcached_bucket) == 256));
    Assert(sizeof(struct mehcached_item) == 24);

    Assert(num_buckets > 0);
    Assert(num_pools > 0);
    Assert(num_pools <= MEHCACHED_MAX_POOLS);
#ifdef MEHCACHED_SINGLE_ALLOC
    if (num_pools != 1)
    {
        fprintf(stderr, "the program is compiled with no support for multiple pools\n");
        Assert(false);
    }
#endif

    size_t log_num_buckets = 0;
    while (((size_t)1 << log_num_buckets) < num_buckets)
        log_num_buckets++;
    num_buckets = (size_t)1 << log_num_buckets;
    Assert(log_num_buckets <= 32);

    table->num_buckets = (uint32_t)num_buckets;
    table->num_buckets_mask = (uint32_t)num_buckets - 1;
#ifndef MEHCACHED_NO_EVICTION
    table->num_extra_buckets = 0;
#else
    table->num_extra_buckets = table->num_buckets / 10;    // 10% of normal buckets
#endif

// #ifdef MEHCACHED_ALLOC_POOL
    {
        // 对于小页来说，是不需要 mehcached_shm_adjust_size 进行 2MB 对齐的
        size_t shm_size = mehcached_shm_adjust_size(sizeof(struct mehcached_bucket) * (table->num_buckets + table->num_extra_buckets));
        // TODO: extend num_buckets to meet shm_size
        size_t shm_id = mehcached_shm_alloc(shm_size, table_numa_node);
        if (shm_id == (size_t)-1)
        {
            INFO_LOG("failed to allocate memory\n");
            Assert(false);
        }
        while (true)
        {
            // table->buckets = mehcached_shm_find_free_address(shm_size);
            // if (table->buckets == NULL)
            //     Assert(false);

            size_t mapping_id  = mehcached_shm_map(shm_id, table->buckets,(void**) &table->buckets, 0, shm_size);
            if (mapping_id != (size_t) -1)
            {
                table->mapping_id = mapping_id;
                break;
            }
        }
        if (!mehcached_shm_schedule_remove(shm_id))
            Assert(false);
    }
// #endif
// #ifdef MEHCACHED_ALLOC_MALLOC
//     {
//         int ret = posix_memalign((void **)&table->buckets, 4096, sizeof(struct mehcached_bucket) * (table->num_buckets + table->num_extra_buckets));
//         if (ret != 0)
//             Assert(false);
//     }
// #endif
    table->extra_buckets = table->buckets + table->num_buckets - 1; // subtract by one to compensate 1-base indices
    // the rest extra_bucket information is initialized in mehcached_table_reset()

    // we have to zero out buckets here because mehcached_table_reset() tries to free non-zero entries in the buckets
    memset(table->buckets, 0, sizeof(struct mehcached_bucket) * (table->num_buckets + table->num_extra_buckets));

    if (!concurrent_table_read)
        table->concurrent_access_mode = 0;
    else if (concurrent_table_read && !concurrent_table_write)
        table->concurrent_access_mode = 1;
    else
        table->concurrent_access_mode = 2;

#ifdef MEHCACHED_ALLOC_POOL
    num_pools = mehcached_next_power_of_two(num_pools);
    table->alloc_id_mask = (uint8_t)(num_pools - 1);
    size_t alloc_id;
    for (alloc_id = 0; alloc_id < num_pools; alloc_id++)
        mehcached_pool_init(&table->alloc[alloc_id], pool_size, concurrent_table_read, concurrent_alloc_write, alloc_numa_nodes[alloc_id]);
    table->mth_threshold = (uint64_t)((double)table->alloc[0].size * mth_threshold);

    table->rshift = 0;
    while ((((MEHCACHED_ITEM_OFFSET_MASK + 1) >> 1) >> table->rshift) > table->num_buckets)
        table->rshift++;
#endif
#ifdef MEHCACHED_ALLOC_MALLOC
    mehcached_malloc_init(&table->alloc);
#endif
#ifdef MEHCACHED_ALLOC_DYNAMIC
    // TODO: support multiple dynamic allocs?
    mehcached_dynamic_init(&table->alloc, pool_size, concurrent_table_read, concurrent_alloc_write, alloc_numa_nodes[0]);
#endif

    mehcached_table_reset(table);

#ifdef NDEBUG
    INFO_LOG("NDEBUG\n");
#else
    INFO_LOG("!NDEBUG (low performance)");
#endif

#ifdef MEHCACHED_VERBOSE
    INFO_LOG("MEHCACHED_VERBOSE (low performance)");
#endif

#ifdef MEHCACHED_COLLECT_STATS
    INFO_LOG("MEHCACHED_COLLECT_STATS (low performance)\n");
#endif

#ifdef MEHCACHED_CONCURRENT
    INFO_LOG("MEHCACHED_CONCURRENT (low performance)");
#endif

    INFO_LOG("MEHCACHED_MTH_THRESHOLD=%lf (%s)", mth_threshold, mth_threshold == 0. ? "LRU" : (mth_threshold == 1. ? "FIFO" : "approx-LRU"));

#ifdef MEHCACHED_USE_PH
    INFO_LOG("MEHCACHED_USE_PH\n");
#endif

#ifdef MEHCACHED_NO_EVICTION
    INFO_LOG("MEHCACHED_NO_EVICTION");
#endif

#ifdef MEHCACHED_ALLOC_POOL
    INFO_LOG("MEHCACHED_ALLOC_POOL\n");
#endif
#ifdef MEHCACHED_ALLOC_MALLOC
    INFO_LOG("MEHCACHED_ALLOC_MALLOC\n");
#endif
#ifdef MEHCACHED_ALLOC_DYNAMIC
    INFO_LOG("MEHCACHED_ALLOC_DYNAMIC");
#endif
    INFO_LOG("num_buckets = %u", table->num_buckets);
    INFO_LOG("num_extra_buckets = %u", table->num_extra_buckets);
    INFO_LOG("pool_size = %zu", pool_size);

    INFO_LOG("\n");
}

static
void
mehcached_table_free(struct mehcached_table *table)
{
    Assert(table);

    mehcached_table_reset(table);

// #ifdef MEHCACHED_ALLOC_POOL
    if (!mehcached_shm_unmap(table->buckets))
        Assert(false);
// #endif
// #ifdef MEHCACHED_ALLOC_MALLOC
//     free(table->buckets);
// #endif

#ifdef MEHCACHED_ALLOC_POOL
    size_t alloc_id;
    for (alloc_id = 0; alloc_id < (size_t)(table->alloc_id_mask + 1); alloc_id++)
        mehcached_pool_free(&table->alloc[alloc_id]);
#endif
#ifdef MEHCACHED_ALLOC_MALLOC
    mehcached_malloc_free(&table->alloc);
#endif
#ifdef MEHCACHED_ALLOC_DYNAMIC
    mehcached_dynamic_free(&table->alloc);
#endif
}

MEHCACHED_END

