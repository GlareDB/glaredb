/*
 * State that's passed around during data generate.
 *
 * Essentially a replacement for global/static variables.
 */

#ifndef STATE_H
#define STATE_H

#include "dss.h"

typedef struct {
    tdef tdefs[10];
    /* Seeds used for data gen, stateful as values are updated */
    seed_t seeds[MAX_STREAM + 1];
} gen_state_t;

seed_t* get_seed(gen_state_t *state, long seed);
void init_gen_state(gen_state_t *state);

#endif
