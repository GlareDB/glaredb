/*
 * $Id: rnd.h,v 1.4 2006/08/01 04:13:17 jms Exp $
 *
 * Revision History
 * ===================
 * $Log: rnd.h,v $
 * Revision 1.4  2006/08/01 04:13:17  jms
 * fix parallel generation
 *
 * Revision 1.3  2006/07/31 17:23:09  jms
 * fix to parallelism problem
 *
 * Revision 1.2  2005/01/03 20:08:59  jms
 * change line terminations
 *
 * Revision 1.1.1.1  2004/11/24 23:31:47  jms
 * re-establish external server
 *
 * Revision 1.1.1.1  2003/08/08 21:50:34  jms
 * recreation after CVS crash
 *
 * Revision 1.3  2003/08/08 21:35:26  jms
 * first integration of rng64 for o_custkey and l_partkey
 *
 * Revision 1.2  2003/08/07 17:58:34  jms
 * Convery RNG to 64bit space as preparation for new large scale RNG
 *
 * Revision 1.1.1.1  2003/04/03 18:54:21  jms
 * initial checkin
 *
 *
 */
/*
 * rnd.h -- header file for use withthe portable random number generator
 * provided by Frank Stephens of Unisys
 */

#ifndef RND_H
#define RND_H

#include "config.h"
#include "dss.h"
#include "state.h"

static long nA = 16807;      /* the multiplier */
static long nM = 2147483647; /* the modulus == 2^31 - 1 */
static long nQ = 127773;     /* the quotient nM / nA */
static long nR = 2836;       /* the remainder nM % nA */

double dM = 2147483647.0;

void dss_random(DSS_HUGE *tgt, DSS_HUGE min, DSS_HUGE max, seed_t *seed);
void row_start(int t, gen_state_t *state);
void row_stop(int t, gen_state_t *state);
void dump_seeds(int t, gen_state_t *state);

#define RANDOM(tgt, lower, upper, stream) dss_random(&tgt, lower, upper, stream)
#define RANDOM64(tgt, lower, upper, stream) dss_random64(&tgt, lower, upper, stream)

/*
 * macros to control RNG and assure reproducible multi-stream
 * runs without the need for seed files. Keep track of invocations of RNG
 * and always round-up to a known per-row boundary.
 */
/*
 * preferred solution, but not initializing correctly
 */
#define VSTR_MAX(len) (long)(len / 5 + (len % 5 == 0) ? 0 : 1 + 1)

#endif
