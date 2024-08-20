FROM rust:1.80-bookworm AS builder

# Get cmake for zlib-ng
#
# Required to build: <https://github.com/rust-lang/libz-sys?tab=readme-ov-file#zlib-ng>
RUN apt-get update -y && apt-get install -y cmake

# Copy in source.
WORKDIR /usr/src/glaredb
COPY . .

RUN cargo install just

# Build release binary.
RUN just build --release

FROM debian:bookworm-slim

# Runtime deps.
RUN apt-get update -y && apt-get install -y openssl ca-certificates openssh-client

# Copy in built stuff.
COPY --from=builder /usr/src/glaredb/target/release/glaredb /usr/local/bin/glaredb

CMD ["glaredb"]
