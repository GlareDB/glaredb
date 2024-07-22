# Notes

Misc notes.

## Hang in NLJ

Query:

```sql
select * from generate_series(1, 3) t1, generate_series(1, 3) t2, generate_series(1, 3) t3
```

Dump:

```
---- slt/standard/cross_join.slt ----
Failed to run SLT
Error source: query failed: Query timed out
---
Pipeline: 0
OPERATORS
[ 0] TableFunction
[ 1] NestedLoopJoin
[ 2] NestedLoopJoin
[ 3] Project (projections = [#0, #1, #2])
[ 4] RoundRobinRepartition
PARTITIONS
[ 0] incomplete: PushTo { operator_idx: 2 }
[ 1] completed: 0ms
[ 2] completed: 0ms
[ 3] completed: 0ms
[ 4] completed: 0ms
[ 5] completed: 0ms
[ 6] completed: 0ms
[ 7] completed: 0ms

Pipeline: 1
OPERATORS
[ 0] TableFunction
[ 1] NestedLoopJoin
PARTITIONS
[ 0] completed: 0ms
[ 1] completed: 0ms
[ 2] completed: 0ms
[ 3] completed: 0ms
[ 4] completed: 0ms
[ 5] completed: 0ms
[ 6] completed: 0ms
[ 7] completed: 0ms

Pipeline: 2
OPERATORS
[ 0] TableFunction
[ 1] NestedLoopJoin
PARTITIONS
[ 0] completed: 0ms
[ 1] completed: 0ms
[ 2] completed: 0ms
[ 3] completed: 0ms
[ 4] completed: 0ms
[ 5] completed: 0ms
[ 6] completed: 0ms
[ 7] completed: 0ms

Pipeline: 3
OPERATORS
[ 0] RoundRobinRepartition
[ 1] QuerySink
PARTITIONS
[ 0] incomplete: PullFrom { operator_idx: 0 }
```

- Partition 0 in pipeline 0 stuck pushing to second nlj.
- NLJs stored buffered batches in a vecdeque, and would wait until the vecdeque
  was empty before accepting another push from the probe side.
- A pull will only take one batch at a time.
- Current partition pipeline execution assumes that if a batch was taken pulled
  from an operator, it's ready to accept another push without pending.
- However the first probe produced more than one batch, and so the second pass
  hung because the pipeline was trying to push to the operator, but the operator
  still had buffered batches.
- Fixed by just buffering a single batch by concatenating batches into one on
  each probe. Possibly inefficient.
- Future work should extend `PollPull` to allow pulling a single batch, multiple
  batches, or even references to external batches (spill).

## CORS & S3

By default, any request from the browser will result in a CORS error (that isn't
properly surfaced yet) when trying to access an S3 bucket.

I copied the `glaredb-test` bucket into a new `glaredb-test-copy` bucket and
added the following CORS rules:

```
[
    {
        "AllowedHeaders": [
            "*"
        ],
        "AllowedMethods": [
            "GET",
            "HEAD"
        ],
        "AllowedOrigins": [
            "*"
        ],
        "ExposeHeaders": [],
        "MaxAgeSeconds": 3000
    }
]
```

(bucket permissions -> cors at bottom)

Easy to add, just need to document. And additional headers will be needed for
writing.

Similar probably needs to happen with GCS: https://cloud.google.com/storage/docs/using-cors#console

# Don't use usize for "long" in protocol messages

Because wasm is 32 bit.

Most likely to hit the issue with microsecond resolution timestamps.
