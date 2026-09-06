ARG YOUKI_BUILDER=docker.io/library/rust:1.96.0-alpine3.22@sha256:9b23044ebd395f416ddb1828d0acf45a18927e617f247a7ca0c787e106fc8f91
FROM ${YOUKI_BUILDER} AS builder
COPY apk.sha256 /inputs/apk.sha256
COPY apks/ /inputs/apks/
RUN cd /inputs/apks && sha256sum -c ../apk.sha256 && apk add --no-network /inputs/apks/*.apk
COPY inputs.env /inputs/inputs.env
COPY source.tar.gz /inputs/source.tar.gz
COPY seccomp-exec.patch /inputs/seccomp-exec.patch
COPY tenant-root.patch /inputs/tenant-root.patch
COPY runtime-log.patch /inputs/runtime-log.patch
COPY executable-permissions.patch /inputs/executable-permissions.patch
RUN . /inputs/inputs.env && \
    echo "$YOUKI_SOURCE_SHA256  /inputs/source.tar.gz" | sha256sum -c - && \
    mkdir /source && tar -xzf /inputs/source.tar.gz -C /source --strip-components=1 && \
    echo "$YOUKI_LOCK_SHA256  /source/Cargo.lock" | sha256sum -c - && \
    test "$(rustc --version | cut -d' ' -f2)" = 1.96.0 && \
    test "$(uname -m)" = aarch64
WORKDIR /source
RUN --network=none . /inputs/inputs.env && \
    echo "$YOUKI_PATCH_SHA256  /inputs/seccomp-exec.patch" | sha256sum -c - && \
    patch --batch --forward --fuzz=0 -p1 -i /inputs/seccomp-exec.patch && \
    echo "$YOUKI_ROOT_PATCH_SHA256  /inputs/tenant-root.patch" | sha256sum -c - && \
    patch --batch --forward --fuzz=0 -p1 -i /inputs/tenant-root.patch && \
    echo "$YOUKI_LOG_PATCH_SHA256  /inputs/runtime-log.patch" | sha256sum -c - && \
    patch --batch --forward --fuzz=0 -p1 -i /inputs/runtime-log.patch && \
    echo "$YOUKI_EXEC_PATCH_SHA256  /inputs/executable-permissions.patch" | sha256sum -c - && \
    patch --batch --forward --fuzz=0 -p1 -i /inputs/executable-permissions.patch && \
    echo "$YOUKI_LOCK_SHA256  /source/Cargo.lock" | sha256sum -c -
ENV LIBSECCOMP_LINK_TYPE=static LIBSECCOMP_LIB_PATH=/usr/lib \
    LIBBPF_SYS_LIBRARY_PATH=/usr/lib PKG_CONFIG_ALL_STATIC=1 \
    CARGO_BUILD_JOBS=2 VERGEN_IDEMPOTENT=1 \
    RUSTFLAGS="-C target-feature=+crt-static -C link-arg=-Wl,-Bstatic -C link-arg=-lzstd -C link-arg=-lgcc -C link-arg=-lc"
RUN cargo +1.96.0 fetch --locked --target aarch64-unknown-linux-musl
RUN --network=none . /inputs/inputs.env && \
    VERGEN_GIT_SHA="$YOUKI_COMMIT+$YOUKI_PATCH_ID+$YOUKI_ROOT_PATCH_ID+$YOUKI_LOG_PATCH_ID+$YOUKI_EXEC_PATCH_ID" cargo +1.96.0 build --frozen --release --locked \
      --target aarch64-unknown-linux-musl -p youki --bin youki \
      --no-default-features --features "$YOUKI_FEATURES"
RUN --network=none cargo +1.96.0 test --frozen --release --locked \
      --target aarch64-unknown-linux-musl -p libcontainer --lib \
      --no-default-features --features v2,cgroupsv2_devices,libseccomp time_ \
      -- --test-threads=1 > /inputs/upstream-tests.txt 2>&1 || \
      { cat /inputs/upstream-tests.txt; exit 1; }
RUN --network=none \
    cargo +1.96.0 test --frozen --release --locked \
      --target aarch64-unknown-linux-musl -p libcontainer --lib \
      --no-default-features --features v2,cgroupsv2_devices,libseccomp \
      namespaces::tests::test_apply_namespaces -- --exact --test-threads=1 \
      >> /inputs/upstream-tests.txt 2>&1 || \
      { cat /inputs/upstream-tests.txt; exit 1; }
RUN --network=none cargo +1.96.0 test --frozen --release --locked \
      --target aarch64-unknown-linux-musl -p libcgroups --lib \
      --no-default-features --features v2,cgroupsv2_devices v2::devices:: \
      -- --test-threads=1 >> /inputs/upstream-tests.txt 2>&1 || \
      { cat /inputs/upstream-tests.txt; exit 1; }
RUN cat /inputs/upstream-tests.txt
RUN --network=none cargo +1.96.0 test --frozen --release --locked \
      --target aarch64-unknown-linux-musl -p libcontainer --lib \
      --no-default-features --features v2,cgroupsv2_devices,libseccomp test_vz_ \
      -- --test-threads=1 --skip test_vz_tenant_root_ --skip test_vz_executable_permissions_ > /inputs/seccomp-exec-tests.txt 2>&1 || \
      { cat /inputs/seccomp-exec-tests.txt; exit 1; }
RUN cat /inputs/seccomp-exec-tests.txt
RUN --network=none cargo +1.96.0 test --frozen --release --locked \
      --target aarch64-unknown-linux-musl -p libcontainer --lib \
      --no-default-features --features v2,cgroupsv2_devices,libseccomp test_vz_tenant_root_ \
      -- --test-threads=1 > /inputs/tenant-root-tests.txt 2>&1 || \
      { cat /inputs/tenant-root-tests.txt; exit 1; }
RUN cat /inputs/tenant-root-tests.txt
RUN --network=none cargo +1.96.0 test --frozen --release --locked \
      --target aarch64-unknown-linux-musl -p youki --bin youki \
      --no-default-features --features v2,cgroupsv2_devices,seccomp observability::tests:: \
      -- --test-threads=1 > /inputs/runtime-log-tests.txt 2>&1 || \
      { cat /inputs/runtime-log-tests.txt; exit 1; }
RUN cat /inputs/runtime-log-tests.txt
RUN --network=none cargo +1.96.0 test --frozen --release --locked \
      --target aarch64-unknown-linux-musl -p libcontainer --lib \
      --no-default-features --features v2,cgroupsv2_devices,libseccomp \
      workload::default::tests::test_vz_executable_permissions_ \
      -- --test-threads=1 --nocapture > /inputs/executable-permissions-tests.txt 2>&1 || \
      { cat /inputs/executable-permissions-tests.txt; exit 1; }
RUN cat /inputs/executable-permissions-tests.txt
RUN --network=none set -eux; . /inputs/inputs.env; \
    mkdir /result && cp target/aarch64-unknown-linux-musl/release/youki /result/youki && \
    readelf -h -l -d /result/youki > /result/elf.txt && cat /result/elf.txt && \
    ! grep -Eq 'INTERP|NEEDED' /result/elf.txt && \
    /result/youki features > /result/features.json && \
    /result/youki --version > /result/version.txt && \
    cp /inputs/inputs.env /inputs/apk.sha256 /result/ && \
    cp /inputs/upstream-tests.txt /result/ && \
    cp /inputs/seccomp-exec.patch /inputs/seccomp-exec-tests.txt /result/ && \
    cp /inputs/tenant-root.patch /inputs/tenant-root-tests.txt /result/ && \
    cp /inputs/runtime-log.patch /inputs/runtime-log-tests.txt /result/ && \
    cp /inputs/executable-permissions.patch /inputs/executable-permissions-tests.txt /result/ && \
    sha256sum Cargo.lock > /result/source-lock.sha256 && \
    cargo +1.96.0 tree --frozen --target aarch64-unknown-linux-musl -p youki \
      --no-default-features --features "$YOUKI_FEATURES" -e features > /result/cargo-features.txt && \
    cd /result && sha256sum youki features.json elf.txt version.txt inputs.env apk.sha256 source-lock.sha256 cargo-features.txt upstream-tests.txt seccomp-exec.patch seccomp-exec-tests.txt tenant-root.patch tenant-root-tests.txt runtime-log.patch runtime-log-tests.txt executable-permissions.patch executable-permissions-tests.txt > evidence.sha256
RUN --network=none set -eu; \
    mkdir /inputs/runtime-log-root; \
    set +e; \
    /result/youki --root /inputs/runtime-log-root --log /result/runtime-log.json \
      --log-format json create --bundle /inputs . \
      > /result/runtime-log-stdout.txt 2> /result/runtime-log-stderr.txt; \
    runtime_log_status="$?"; set -e; \
    echo "$runtime_log_status" > /result/runtime-log-exit-status.txt; \
    test "$runtime_log_status" = 1; \
    test -s /result/runtime-log.json; \
    cd /result; \
    sha256sum runtime-log.json runtime-log-stdout.txt runtime-log-stderr.txt runtime-log-exit-status.txt >> evidence.sha256
FROM scratch AS artifact
COPY --from=builder /result/ /
