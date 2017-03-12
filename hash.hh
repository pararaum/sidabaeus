#ifndef __HASH_HH_2017__
#define __HASH_HH_2017__
#include <stdint.h>
#include <stddef.h>

uint32_t jenkins_one_at_a_time_hash(const uint8_t* key, size_t length);
uint32_t djb2_hash(const uint8_t *data, size_t length);
uint32_t djb2xor_hash(const uint8_t *data, size_t length);
uint32_t sbox_hash(const uint8_t *data, size_t length);

#endif
