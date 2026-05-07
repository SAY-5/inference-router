# Multi-stage build: a Debian builder, then a minimal runtime.
FROM debian:bookworm-slim AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake ca-certificates git \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY CMakeLists.txt ./
COPY src ./src
COPY tools ./tools
COPY tests ./tests
COPY bench ./bench
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DIR_BUILD_TESTS=OFF -DIR_BUILD_BENCH=OFF \
    && cmake --build build -j --target inference-router backend-echo \
    && strip build/inference-router build/backend-echo

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/build/inference-router /usr/local/bin/inference-router
COPY --from=builder /src/build/backend-echo /usr/local/bin/backend-echo
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/inference-router"]
CMD ["--listen", "0.0.0.0:8080", "--backend", "backend:9090"]
