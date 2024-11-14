# Query graph

Overview of the current operator interfaces as of May 2024 alongside
alternatives considered during the implementation of various operators.

## Comments on the previous interfaces

Physical operators now currently implement a push-based interface, and a
separate struct helps with execution across some number of operators. At a
high-level, this is what we want, and the current interfaces I think are a
decent prototype. However there's a drawback where operators require a lot of
unnecessary synchronization, making state management within operators tedious.

### Physical operators

These interfaces define the core execution of individual steps during query
execution.

```rust
/// Result of a push to a Sink.
///
/// A sink may not be ready to accept input either because it's waiting on
/// something else to complete (e.g. the right side of a join needs to the left
/// side to complete first) or some internal buffer is full.
pub enum PollPush {
    /// Batch was successfully pushed.
    Pushed,

    /// Batch could not be processed right now.
    ///
    /// A waker will be registered for a later wakeup. This same batch should be
    /// pushed at that time.
    Pending(DataBatch),

    /// This sink requires no more input.
    ///
    /// Upon receiving this, the operator chain should immediately call this
    /// sink's finish method.
    Break,
}

/// Result of a pull from a Source.
#[derive(Debug)]
pub enum PollPull {
    /// Successfully received a data batch.
    Batch(DataBatch),

    /// A batch could not be be retrieved right now.
    ///
    /// A waker will be registered for a later wakeup to try to pull the next
    /// batch.
    Pending,

    /// The source has been exhausted for this partition.
    Exhausted,
}

pub trait Sink: Sync + Send + Explainable + Debug {
    /// Number of input partitions this sink can handle.
    fn input_partitions(&self) -> usize;

    fn poll_push(
        &self,
        task_cx: &TaskContext,
        cx: &mut Context,
        input: DataBatch,
        partition: usize,
    ) -> Result<PollPush>;

    fn finish(&self, task_cx: &TaskContext, partition: usize) -> Result<()>;
}

pub trait Source: Sync + Send + Explainable + Debug {
    /// Number of output partitions this source can produce.
    fn output_partitions(&self) -> usize;

    fn poll_pull(
        &self,
        task_cx: &TaskContext,
        cx: &mut Context,
        partition: usize,
    ) -> Result<PollPull>;
}

pub trait PhysicalOperator: Sync + Send + Explainable + Debug {
    /// Execute this operator on an input batch.
    fn execute(&self, task_cx: &TaskContext, input: DataBatch) -> Result<DataBatch>;
}
```

Good:

- `PollPush` and `PollPull`. These are pretty flexible and map nicely to
  `std::task::Poll` (just with a bit more ergnomics around returning a
  `Result`).
- A pretty clear split between stateful (`Sink` + `Source`) operators like
  joins, and stateless (`PhysicalOperator`) like filter. Naming could be better
  though.

Bad:

- Stateful operator implementations require that they keep track of the state
  internal to the operator. This is the source of unecessary synchronization. A
  partition should not have to synchronize if its work can be done independent
  of any other partition.
  
  For example, the build side of the nested loop join:
  
  ```rust
  #[derive(Debug)]
  pub struct PhysicalNestedLoopJoinBuildSink {
      /// Partition-local states.
      states: Arc<Vec<Mutex<LocalState>>>,

      /// Number of partitions we're still waiting on to complete.
      remaining: AtomicUsize,
  }
  ```

  The build side of the nested loop join should not need to synchronize until
  that partitions input is complete. Roughly, the `poll_push` implementation
  should never lock, while the `finish` would lock to coordinate with a global
  state.

Unknown:

- The `TaskContext` that's passed in to the various methods was entirely to
  provide operators with handle for modifying the calling session (e.g. setting
  a session variable). We need that functionality, but the way it's currently
  being done probably isn't it.
  
  I also originally had the idea of using `TaskContext` to store performance
  metrics of the operators, but I no longer think that's a good idea
  (`TaskContext` can't be mutable and so there would need to be more
  synchronization). I plan to address the performance metrics later in this
  document.
  
### Operator chain

An operator chain is essentially a subset of the query.

```rust
/// An operator chain represents a subset of computation in the execution
/// pipeline.
///
/// During execution, batches are pulled from a source, ran through a sequence
/// of stateless operators, then pushed to a sink.
#[derive(Debug)]
pub struct OperatorChain {
    /// Sink where all output batches are pushed to.
    sink: Box<dyn Sink>,

    /// Sequence of operators to run batches through. Executed left to right.
    operators: Vec<Box<dyn PhysicalOperator>>,

    /// Where to pull originating batches.
    source: Box<dyn Source>,

    /// Partition-local states.
    states: Vec<Mutex<PartitionState>>,
}
```

Good:

- I think the general approach is good in terms of having a core unit of
  execution.
  
Bad:

- This handles _all_ partitions for this subset of the query. I think there's a
  possibility to break this down such that we can avoid the mutexes. Ideally
  this requires no mutexes, and a thread is able to operator on this with
  mutable access.

Unknown:

### Pipeline

Essentially the full query represented in multiple operator chains. This is what
the scheduler currently accepts.

```rust
#[derive(Debug)]
pub struct Pipeline {
    /// The operator chains that make up this pipeline.
    pub chains: Vec<Arc<OperatorChain>>,
}
```

Good:

- Simple. There's no fancy routing that has to happen as each operator can be
  called arbitrarily thanks to the `Poll...` stuff. If there's nothing to do,
  that chain will be called agains once it is ready to be executed.
  
  This simplicity actually ends up making the scheduler pretty simple too
  (currently `scheduler.rs` is 141 lines long).

Bad:

Unknown:

## Current design

Operator logic and operator states should be separate. By having the state
separate from the logic (the actual operator itself), we can reduce
synchronization across partitions, and provide better DX.

### Global/local states

Each (stateful) operator will define at least two states that will be used
during execution; the "partition" state and the "operator" state. Partition
states are local to a single partition during execution, while the operator
state is shared among all partitions for a single operator instance.

For example, the build-side hash join operator would have a partition-local hash
table in its partition state. During execution, the operator will receive a
mutable reference to its partition-local state, allowing direct modification
with no synchronization.

And for operator states, the build-side hash join operator would have a global
hash table that's written to from each partition with each partition's local
hash table. During execution, the operator will receive a shared reference to
the global state. Modifying the global state will require internal mutation
through mutexes/atomics/etc.

Since we will only support a fixed number of operators, each operator will have
a variant in each of the operator and partition state enums:

```rust
pub enum OperatorState {
    PhysicalHashJoin(PhysicalHashJoinOperatorState),
    ...
}

pub enum PartitionState {
    PhysicalHashJoin(PhysicalHashJoinPartitionState),
    ...
}
```

### Operator interfaces

```rust
pub trait PhysicalOperator: Sync + Send + Explainable + Debug {
    /// Try to push a batch for this partition.
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush>;

    /// Finalize pushing to partition.
    ///
    /// This indicates the operator will receive no more input for a given
    /// partition, allowing the operator to execution some finalization logic.
    fn finalize_push(
        &self,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<()>;

    /// Try to pull a batch for this partition.
    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollPull>;
}
```

Key differences:

- Operators receive both a mutable partition state, and a shared operator state.
  Operator implementations deal with the logic, with any state needed being
  stored on the provided state objects.
- Operators are oblivious to number of input/output partitions. Operators are
  constructed with "static" parameters (e.g. join keys for hash joins). States
  are what will determine the number of input/output partitions.

`PollPush` and `PollPull` will remain mostlye the same.

In the context of distributed execution, it's important that we're able to
construct new states given just an operator, but the number of partitions to
execute on should be determined by the node actually executing the pipeline.

### Partition pipeline

Partition pipelines are a higher level struct responsible for managing the
relationships between states and operators for a single partition. This struct
would have: Arc references to operators, Arc references to operator (global)
state, exclusive references to partitions (local) states. Executing a partition
pipeline will attempt to push data as far as it can through the chain of
operators until it hits a "Pending" state, at which point it'll be rescheduled
for later execution once it can continue to make progress.

When it comes to the scheduler, partition pipelines are the smallest unit of
work. Partition pipelines implement a `poll_execute` method which behaves very
similarly to a Rust `Future`. The scheduler continually calls `poll_execute` to
make progress, and the partition pipeline will pull a batch from the "source"
operator, and attempt to push it through a sequence of operators until it's
pushed to a "sink" operator. Once the batch has been pushed to the "sink"
operator, its internal state gets reset to then begin pulling from the "source"
operator. This gets repeated until the source is exhausted.

Any operator in the partition pipeline may return "pending" on either
`poll_push`, or `poll_pull`. The partition pipeline will bubble this up to the
scheduler, and the thread currently executing the partition pipeline will return
from the execution loop so that it can pick up another partition pipeline to
begin executing. The original partition pipeline will resume execution when an
operator signals there's more work to be done. This is done by calling `wake` on
a stored `Waker`. The waker implementation is a `PartitionPipelineWaker` which
internally holds a reference to the partition pipeline, whose `wake`
implementation schedules that partition pipeline for execution.

### Pipeline

Even higher level struct that holds multiple partition pipelines representing a
portion of query execution. When a pipeline is passed to the scheduler, the
scheduler just pulls out the partition pipelines and begins executing them
independently. Dependencies between pipelines are implicitly handle by partition
pipelines returning "pending" if there's no work to do yet.

Pipelines contain a consistent number of partitions end-to-end. The number of
inputs into a pipeline equals the number of outputs from a pipeline. When a
different number of partitions are required (e.g. a source produces 4
partitions, but we want execution to happen across 8 partitions), a round-robin
repartitioning operator is added to the _current_ pipeline, and a new pipeline
is created whose "source" batches are the output of the repartition operator.
This new pipeline is then used during planning up until a join (ish) or
repartition is reached. So planning may produce any number of partitions, which
can all be thrown to scheduler without care.

### Query graph

More of a tree, but just holds a bunch of pipelines.

More graph like execution might happen in the case of recursive CTEs.

## Current design rationale

Various notes on the rationale of the current design. This will be appended to
as changes are made, with some sections potentially moving to _Design
alternatives_ if we move away from it.

### "Polling" operators

Physical operators implement a `poll_push` and `poll_pull` method, which can be
seen as a very specialized version of a Rust Future.

We want a way to say "we don't have a batch ready yet" or "we don't have room
for your batch yet", and by being able to return a `Poll...` object from these
functions, we can signal to the higher level partition pipeline what action
needs to be taken next. For example, the build side of a join might not have
completed, and so the it doesn't make sense to continue to try to push to the
probe side. By allowing the operator to return a `PollPull::Pending(_)` object,
it can let the pipeline know that it can't yet accept batches.

We're using async Rust primitives here (`Context`, `Waker`) which provides the
interfaces for triggering re-execution of pipelines once there's work to be
done. This also gives us a very straightforward path to integrating async
functions at the "edge" of the system (e.g. async http requests for fetching
data) since we'll just be calling the required `poll_...` methods with the
`Context` and `Waker` we're already passing around.

Essentially this makes our scheduler a very specialized async runtime.

## Design alternatives

### Using the previous implementation of physical operators

The idea was good, but having the state+logic live on the operator itself led to
a lot synchronization needing to happen across partitions, as well as a
confusing mix of logica between what was "local" to a partition, and what was
shared between partitions.
