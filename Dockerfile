FROM rust AS builder

WORKDIR /usr/src/rayexec
COPY . .

RUN ./scripts/install_protoc_linux.sh
RUN cargo build --release --bin rayexec_server

FROM debian:bookworm-slim

COPY --from=builder /usr/src/rayexec/target/release/rayexec_server /usr/local/bin/rayexec_server

CMD ["rayexec_server"]
