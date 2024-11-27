#include "config.h"
#include "dss.h"
#include "dsstypes.h"
#include "state.h"

void generate_table_data(int table_idx, DSS_HUGE row_count) {}

void foo_function(void) {}

/*
 * generate a particular table
 */
void gen_tbl(int tnum, DSS_HUGE row_count, DSS_HUGE offset) {
    order_t o;
    supplier_t supp;
    customer_t cust;
    part_t part;
    code_t code;

    DSS_HUGE i;

    for (i = offset; offset < row_count; offset++) {
        row_start(tnum);

        switch (tnum) {
        case LINE:
        case ORDER:
        case ORDER_LINE:
            mk_order(i, &o, 0);
            // Append
            break;
        case SUPP:
            mk_supp(i, &supp);
            // Append
            break;
        case CUST:
            mk_cust(i, &cust);
            // Append
            break;
        case PSUPP:
        case PART:
        case PART_PSUPP:
            mk_part(i, &part);
            // Append
            break;
        case NATION:
            mk_nation(i, &code);
            // Append
            break;
        case REGION:
            mk_region(i, &code);
            // Append
            break;
        }

        row_stop(tnum);
    }
}

void generate_data(double scale) {
    struct GenState state;
    DSS_HUGE i;
    DSS_HUGE rowcnt = 0;

    init_gen_state(&state);

    for (i = PART; i <= REGION; i++) {
        if (table & (1 << i)) {
            if (i < NATION)
                rowcnt = tdefs[i].base * scale;
            else
                rowcnt = tdefs[i].base;
            gen_tbl((int)i, rowcnt, 0);
        }
    }
}
