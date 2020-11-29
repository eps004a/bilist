#pragma once

#include <sys/types.h>

/**
 * From wikipedia - xorshift 64 bits
*/
struct xorshift64_state {
  u_int64_t a;
};

inline static u_int64_t xorshift64(struct xorshift64_state *state)
{
	u_int64_t x = state->a;
	x ^= x << 13;
	x ^= x >> 7;
	x ^= x << 17;
	return state->a = x;
}

/**
 * Pseudo random 64bit initialiser and generator
*/
struct prand {
    struct xorshift64_state state;
};

inline static void pseed(struct prand *prand, u_int64_t seed)
{
    prand->state.a = seed;
}

inline static u_int64_t prand(struct prand *prand)
{
    return xorshift64(&(prand->state));
}

inline static u_int32_t prand32(struct prand *prandseed)
{
	long result = prand(prandseed);

	return result % (1<<31);
}
