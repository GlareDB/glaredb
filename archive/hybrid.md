# Notes on hybrid execution

Notes on hybrid execution in the context of rayexec. While not an immediate
focus, it's important that there's an idea of what this might look like as it's
critical to the glaredb product.

## Hybrid execution right now

Hybrid execution in glaredb currently relies on parsing, planning, and
optimization all on the client side.

During SQL planning, datafusion (and our fork of it) will ask for a table
provider from the plan context. This table provider requires calling out to the
external services in order to retrieve the schema, which the planner needs when
building the logical plan. What we do is we'll make a request to the Cloud
instance to build the actual table provider on the remote side, then return an
id that references that provider back to the client along with the table's
schema. The client then builds a "stub" table provider using that id and schema,
and planning continues as normal.

During physical planning, which also happens client-side, the "stub" table
provider is transformed into a "stub" execution plan. Tables that are deemed
local have the execution plans built normally, but then are wrapped in an
addition plan which hooks up the local and remote side. When we serialize and
send the plan to the remote instance, the leafs of the plan that reference local
files are stripped out and collected, and only the nodes "above" these leafs are
serialized. The remote node then receives the plan, transforms the "stub"
execution plans (the plans that reference exeternal tables) into proper
execution plans using the table providers that were created during planning. And
then these execution plans await execution.

Execution is driven completely by stream pulling. The client has the root of its
plan a node which internally drives everything forward during polling, including
streaming batches from the client to the remote instances, and from the remote
instance to the client.

For example:

```sql
SELECT * FROM 'local.csv' INNER JOIN postgres_table USING (a);
```

Assuming `local.csv` is left side of join...

1. (Client) - Pull on output stream
2. (Client) - Pull on result stream from remote
3. (Remote) - Pull on upload stream from client for `local.csv`
4. (Remote) - Pull on stream from postgres
5. (Client) - Receive batch from remote
6. Repeat steps as needed until completion

## Hybrid execution in rayexec

### Definitions

- **Sink**: Some place to push batches to. Can implement backpressure by
  implementing `poll_ready` as needed.
- **Source**: Some place to pull batches from.
- **PhysicalOperator**: A stateless execution operator that produces exactly one
  batch output for one batch input.
- **OperatorChain**: A chain of exactly 1 Sink, 1 Source, and some number of
  PhysicalOperators in between.
- **Pipeline**: Some number of OperatorChains that are arbitrarily execution.
  Chain dependendencies are implicit and provided by the Source or Sink yielding
  as appropriate. For example, if a chain's source is the output of a join, and
  join hasn't started producing batches, it will yield and the next chain will
  attempt to be execution. Chains that have yielded will be executed again when
  either their Source or Sink have triggered a wakeup.

### Phase 1

Goals:

- Parsing and logical planning locally.
- Optimization and physical planning remote.
- Independent execution on local and remote sides (with some backpressure)

Phase 1 of hybrid exec will include doing the entirety of logical planning
locally, with a very similar system to our current implementation of making a
call out to the remote node during planning to get the schema.

Once the logical plan is completed, any leaf nodes referencing local files will
be replaced with a stub node containing the relevant information about the file.
The logical plan will then get serialized and sent to the remote node.

The remote node will be responsible for optimization and physical planning.
Physical planning will result in two pipelines, one that's expected to be
executed on the remote side, and one that's expected to be executed locally.
The local pipeline is serialized and sent to the client. Both sides can begin
executing immediately, with batches going to some buffer, and the buffer
being the source backpressure.

The client's pipeline will have some number of `Source` implementations which
pulls batches from the remote side (over HTTP GET or grpc). It will also have
some number of `Sink` implementations that push batches to the remote side (over
HTTP POST or grpc). Both of these will make use of the existing scheduler
implementation to process operators in parallel and yield when needed.

The remote instance's pipeline will have some number `Source` implementations
which read batches from a buffer that gets filled with batches sent by the
client. It will also have some number of `Sink` implementations that hold
outgoing batches that are awaiting reads by the client. Like above, the existing
scheduler implementation will be used for parallelism and yielding.

### Phase 2

Goals:

- Partial logical planning

The aim is to remove needing to call out to the remote instance at all during
logical planning. The idea is that the client will build up as much of the
logical plan as it can, and then send off the partial plan (and remaining ast
info) to the remote node to complete planning.

Generally this will look like a lot of "unknown" nodes in the logical plan and
expressions trees, containing the relevant part of the ast.

Physical planning and execution will remain identical to phase 1.
