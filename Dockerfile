FROM rust:1.69-bookworm AS builder

# Copy in source.
WORKDIR /usr/src/glaredb
COPY . .

RUN apt-get update && apt-get install -y openssl ca-certificates

# Get protoc.
RUN wget https://github.com/protocolbuffers/protobuf/releases/download/v23.1/protoc-23.1-linux-x86_64.zip \
    && unzip protoc-23.1-linux-x86_64.zip -d $HOME/.protoc

# Build release binary.
RUN PROTOC="$HOME/.protoc/bin/protoc" \
    cargo build -r --bin glaredb

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
