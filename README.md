# rayexec

An experimental push-based execution engine.

## Architecture

Rationale and overview of current/planned architecture.

### Composability

Rayexec aims to be _somewhat_ composable in that if we choose to integrate parts
of this into GlareDB, it should be a relatively lightweight refactor. Planning,
optimizations, and execution should not rely on a central "Session" object, and
should easily be constructed independently from one another.

General purpose composability is not currently a goal.

### Async

- No async during planning or optimizing.
- Async during execution provided by scheduler.
- No tokio.
- Table binding could be made async in the future.

### SQL parser

Custom parser based heavily on `sqlparser-rs` is used. This will let us have
much more control over parsing while also being able to eschew certain features
from `sqlparser-rs` that doesn't benefit us.

### Logical planning

- Bind tables and column using some bind context. Tables and columns will be
  given numeric identifies, all future planning will use that identifier in
  place of the column name.
- Bind contexts will be scoped, but have a reference to the outer scope to
  support LATERAL subqueries.

### Logical operators

- Tree, each node knows about its children.
- Each node knows its output schema.
  
### Physical operators

- Pushed-based.
- Operators do no know about its parents/children, just that it accepts inputs
  and produces outputs.
- Operator-level parallelism through partitions.
- Tightly coupled to scheduling.

### Scheduler

- Accepts a "Pipeline", executes all operator to completion.
- Parallelizes operators by cloning them to match the partition count. Each
  partition is executed separately.

### Optimizer

- Only logical plan optimizations.
- No physical plan optimizations. Stuff like join selection should happen during
  planning.

### Data scans

- Everything's a function.
- Dynamic dispatch based on arguments provided during binding.
  - `read_parquet('./some/path.parquet)` => `ReadParquetLocal`
  - `read_parquet('s3://bucket/some/path.parquet')` => `ReadParquetRemote`
- Function implementation implements `PhysicalOperator`.
  - `Source` for producing batches.
  - `Sink` currently errors. Idk if there's something we'd want to do there
    (table in/out functions).

