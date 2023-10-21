FROM rust:1.73-bookworm AS builder

# Copy in source.
WORKDIR /usr/src/glaredb
COPY . .

RUN apt-get update && apt-get install -y openssl ca-certificates
RUN cargo install just
# Build release binary.
RUN just build --release

# Generate certs.
RUN ./scripts/gen-certs.sh

FROM debian:bookworm-slim

# Runtime deps.
RUN apt-get update -y && apt-get install -y openssl ca-certificates openssh-client

# Copy in built stuff.
COPY --from=builder /usr/src/glaredb/target/release/glaredb /usr/local/bin/glaredb
RUN mkdir -p /certs
COPY --from=builder /usr/src/glaredb/server.crt /certs/.
COPY --from=builder /usr/src/glaredb/server.key /certs/.

CMD ["glaredb"]
