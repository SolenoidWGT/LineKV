#include <unistd.h>
#include <sys/mman.h>
#include <stdio.h>
#include <malloc.h>
int main()
{   
    void * a = malloc(1024);
    void * p = mmap(a, 1024, PROT_WRITE | PROT_READ, MAP_FIXED | MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    printf("%p, %p\n", a, p);
    return 0;
}