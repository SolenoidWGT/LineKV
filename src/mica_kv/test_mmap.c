/*
 * @Author: your name
 * @Date: 2022-02-21 20:36:29
 * @LastEditTime: 2022-02-22 23:23:45
 * @LastEditors: your name
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: /star_midd/src/mica_kv/test_mmap.c
 */
#include <unistd.h>
#include <sys/mman.h>
#include <stdio.h>
#include <malloc.h>

int main()
{   
    //void * a = malloc(1024);
    //void * p = mmap(a, 1024, PROT_WRITE | PROT_READ, MAP_FIXED | MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    // INFO_LOG("%p, %p", a, p);
    return 0;
}