/*
 * State that's passed around during data generate.
 *
 * Essentially a replacement for global/static variables.
 */

#ifndef STATE_H
#define  STATE_H

#include "dss.h"

typedef struct {
    tdef tdefs[10];
    /* Seeds used for data gen, stateful as values are updated */
    seed_t seeds[MAX_STREAM + 1];
} gen_state_t;

seed_t* get_seed(gen_state_t *state, long seed) {
    return &state->seeds[seed];
}

void init_gen_state(gen_state_t *state) {
    // Replaces the static instantion in driver.c
    //
    // Also sets the callbacks to NULL, we'll be handling those ourselves.
    state->tdefs[PART]       = (tdef){"part.tbl", "part table", 200000, NULL, NULL, PSUPP, 0};
    state->tdefs[PSUPP]      = (tdef){"partsupp.tbl", "partsupplier table", 200000, NULL, NULL, NONE, 0};
    state->tdefs[SUPP]       = (tdef){"supplier.tbl", "suppliers table", 10000, NULL, NULL, NONE, 0};
    state->tdefs[CUST]       = (tdef){"customer.tbl", "customers table", 150000, NULL, NULL, NONE, 0};
    state->tdefs[ORDER]      = (tdef){"orders.tbl", "order table", 150000, NULL, NULL, LINE, 0};
    state->tdefs[LINE]       = (tdef){"lineitem.tbl", "lineitem table", 150000, NULL, NULL, NONE, 0};
    state->tdefs[ORDER_LINE] = (tdef){"orders.tbl", "orders/lineitem tables", 150000, NULL, NULL, LINE, 0};
    state->tdefs[PART_PSUPP] = (tdef){"part.tbl", "part/partsupplier tables", 200000, NULL, NULL, PSUPP, 0};
    state->tdefs[NATION]     = (tdef){"nation.tbl", "nation table", NATIONS_MAX, NULL, NULL, NONE, 0};
    state->tdefs[REGION]     = (tdef){"region.tbl", "region table", NATIONS_MAX, NULL, NULL, NONE, 0};
}

#endif
