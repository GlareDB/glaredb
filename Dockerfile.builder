FROM rust:1.69-bookworm AS builder

# Copy in source.
COPY . .

# Get deps!
RUN apt update -y && apt install -y protobuf-compiler

# Build necessary binaries.
RUN cargo build --bin glaredb
RUN cargo build --bin pgprototest
RUN cargo build --bin sqllogictests
RUN cargo test --no-run

CMD ["ls"]
