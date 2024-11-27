#ifndef DBGEN_H
#define DBGEN_H
#endif

// TODO: Remove me? Or go ahead and hard code?
/* Define DBNAME, LINUX, and TPCH.
 *
 * These would have been set in the makefile, but we're not using that.
 * These values don't really matter, just we need them so that everything's
 * configured appropriate (e.g. defining DSS_HUGE for 64 bit ints).
 */
#ifndef DBNAME
#define DBNAME "dss"
#endif

#ifndef LINUX
#define LINUX
#endif

#ifndef SQLSERVER
#define SQLSERVER
#endif

#ifndef TPCH
#define TPCH
#endif

#include "config.h"
#include "dss.h"
#include "dsstypes.h"


void generate_table_data(int table_idx, DSS_HUGE row_count) {}

void foo_function(void);
