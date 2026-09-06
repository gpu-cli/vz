#!/bin/sh
# Native compiled-CLI audit fixture, not an OCI payload or Docker/Machine test.
set -eu
umask 077
test "$(id -u)" = 0
audit_root=/var/lib/docker/runtime-audit
audit_session=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
audit_cid=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
test ! -e "$audit_root" && test ! -L "$audit_root"
mkdir -p /var/lib/docker
mkdir -m 700 "$audit_root" /inputs/runtime-audit-root /inputs/runtime-audit-invalid-bundle
cat /proc/sys/kernel/random/boot_id > /result/runtime-audit-boot-id.txt
audit_boot=$(cat /result/runtime-audit-boot-id.txt)
printf '{"schema_version":1,"session_id":"%s","boot_id":"%s"}\n' "$audit_session" "$audit_boot" > "$audit_root/enrollment.json"
: > "$audit_root/events.jsonl"
printf 'complete\n' > "$audit_root/status"
chmod 600 "$audit_root/enrollment.json" "$audit_root/events.jsonl" "$audit_root/status"
audit_metadata() {
    stat -c '%n|%u|%g|%a|%h|%d|%i' "$audit_root" "$audit_root/enrollment.json" "$audit_root/events.jsonl" "$audit_root/status"
}
audit_metadata > /result/runtime-audit-metadata-before.txt
audit_run() {
    audit_case=$1
    audit_expected=$2
    shift 2
    printf '%s\0' /bin/busybox timeout -s KILL 30 /result/youki --root /inputs/runtime-audit-root "$@" > "/result/runtime-audit-$audit_case.argv"
    set +e
    VZ_NATIVE_AUDIT_ENV_CANARY=vz-native-audit-env-canary-v1 /bin/busybox timeout -s KILL 30 /result/youki --root /inputs/runtime-audit-root "$@" \
        > "/result/runtime-audit-$audit_case.stdout" 2> "/result/runtime-audit-$audit_case.stderr"
    audit_status=$?
    set -e
    printf '%s\n' "$audit_status" > "/result/runtime-audit-$audit_case.exit-status.txt"
    test "$audit_status" = "$audit_expected"
}
audit_run version 0 --version
audit_run create 1 create --bundle /inputs/runtime-audit-invalid-bundle "$audit_cid"
audit_run exec 255 exec --env VZ_NATIVE_AUDIT_EXEC_CANARY=vz-native-audit-exec-env-canary-v1 "$audit_cid" /vz-native-audit-argv-canary-v1
audit_run run 255 run --bundle /inputs/runtime-audit-invalid-bundle "$audit_cid"
audit_metadata > /result/runtime-audit-metadata-after.txt
cmp /result/runtime-audit-metadata-before.txt /result/runtime-audit-metadata-after.txt
cp "$audit_root/enrollment.json" /result/runtime-audit-enrollment.json
cp "$audit_root/events.jsonl" /result/runtime-audit-events.jsonl
cp "$audit_root/status" /result/runtime-audit-status.txt
cmp /result/runtime-audit-version.stdout /result/version.txt
test "$(cat /result/runtime-audit-status.txt)" = complete
