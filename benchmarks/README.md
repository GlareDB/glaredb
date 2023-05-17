# Benchmarking

Running benchmarks against GlareDB can be done with the `bench_runner` binary.
This directory contains SQL files and scripts for setting up benchmarks and
executing benchmarks with `bench_runner`.

Note to make writing scripts and load SQL queries easy, it's assumed that
everything is executed from the root of the repo.

When running `bench_runner`, logging is not initialized to make reading the
output easier. The output will look like the following:

```
name           , run            , duration       
1.sql          , 1              , 0.138352       
10.sql         , 1              , 0.22943        
11.sql         , 1              , 0.11337        
12.sql         , 1              , 0.080102       
13.sql         , 1              , 0.292445       
14.sql         , 1              , 0.067516       
15.sql         , 1              , 0.08575        
16.sql         , 1              , 0.09127        
17.sql         , 1              , 0.766149       
18.sql         , 1              , 0.591473       
19.sql         , 1              , 0.143249       
2.sql          , 1              , 0.109643       
20.sql         , 1              , 0.230755       
21.sql         , 1              , 0.326055       
22.sql         , 1              , 0.067345       
3.sql          , 1              , 0.115398       
4.sql          , 1              , 0.062349       
5.sql          , 1              , 0.144521       
6.sql          , 1              , 0.043549       
7.sql          , 1              , 0.235614       
8.sql          , 1              , 0.132384       
9.sql          , 1              , 0.210942       
Total: 4.277661s
```

`name` is the name of the file with the benchmark query, `run` is which run the
timing is for, and `duration` is time in seconds it took for the query to complete.

The `--runs` flag may be provided to `bench_runner` execute benchmark queries
some number of times.

## TPC-H

Before running the TPC-H queries, the relevant files will need to be downloaded.
The `download_data.sh` script does just that.

For example, running TPC-H at scale factor 10:

``` shell
$ SCALE_FACTOR=10 ./benchmarks/tpch/download_data.sh
$ cargo run -r --bin bench_runner bench_runner --load ./benchmarks/tpch/load_s10.sql --runs 1 ./benchmarks/tpch/queries/*
```

