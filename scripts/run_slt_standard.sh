#!/usr/bin/env bash

# Script to run standard SLT tests, with some skips until those are fixed.

# slt/standard/aggregates/distinct.slt                 -- Need to re-implement distinct hash table.
# slt/standard/functions/scalars/list_comparisons.slt  -- Need list comparisons
# slt/standard/functions/scalars/list_values.slt       -- Cast different decimal precision/scale to same in sig resolution
# slt/standard/functions/scalars/l2_distance.slt       -- Cast decimal to float in list in sig resolution
# slt/standard/functions/table/unnest_list.slt         -- Lateral inputs (list equality)
# slt/standard/select/unnest.slt                       -- Table execute functions in project
cargo test slt/standard -- \
      --skip slt/standard/aggregates/distinct.slt \
      --skip slt/standard/functions/scalars/list_comparisons.slt \
      --skip slt/standard/functions/scalars/list_values.slt \
      --skip slt/standard/functions/scalars/l2_distance.slt \
      --skip slt/standard/functions/table/unnest_list.slt \
      --skip slt/standard/select/unnest.slt

