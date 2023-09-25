# TPCH Benchmarks for GlareDB and similar systems

## Usage

### Download submodule

```sh
> git submodule update --init --recursive
```

### Downloading data

```sh
> just gen_scale_factor 1 # generate data for scale factor 1
```

### Running benchmarks

```sh
> just run $NUM_RUNS $SCALE_FACTOR
```

For example if you wanted to run the queries 10 times for scale factor 1:

```sh
> just run 10 1
```

If you want to run a single system, you can do so by specifying the system name:

```sh
> just run_glaredb $SCALE_FACTOR
```

These benchmarks are output to both the console, and to a file `timings.csv`


### Cleaning up
  
```sh
> just clean
```