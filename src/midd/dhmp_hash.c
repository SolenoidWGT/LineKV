#include "dhmp.h"
#include "dhmp_log.h"
#include "dhmp_hash.h"

#include "murmur3_hash.h"

hash_func dhmp_hash;

void dhmp_hash_init() 
{
    dhmp_hash = MurmurHash3_x86_32;
}


