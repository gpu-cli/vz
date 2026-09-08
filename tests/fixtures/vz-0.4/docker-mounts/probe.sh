# vz docker-mounts probe: report one storage fact per line, never decide it.
# Runs as the container's PID 1 under `/bin/busybox sh -c` from the digest-pinned
# developer probe rootfs, so every applet is addressed through /bin/busybox. The
# caller selects the case in $1 and the owner token in $2; this script only
# reports what it observed and exits non-zero if it could not observe at all.
# A write that the host expects to be refused is reported as a refusal, never
# retried, and never silently downgraded to success.
set -eu
bb=/bin/busybox
case_name=$1
owner=$2
say() { printf 'VZMOUNT %s %s=%s\n' "$owner" "$1" "$2"; }
# Report the outcome of one write without letting a refusal end the script.
attempt() {
  _key=$1
  _path=$2
  # 2>/dev/null must precede the write: redirections are applied in order, and
  # a refused `>` is reported by the shell itself, not by printf.
  if printf '%s' "$owner" 2>/dev/null > "$_path"; then say "$_key" written; else say "$_key" refused; fi
}
say case "$case_name"
say uid "$("$bb" id -u)"
say gid "$("$bb" id -g)"
case "$case_name" in
  bind)
    say input_sha256 "$("$bb" sha256sum /workspace/input | "$bb" cut -d' ' -f1)"
    say input_bytes "$("$bb" wc -c < /workspace/input | "$bb" tr -d ' ')"
    "$bb" sha256sum /workspace/input | "$bb" cut -d' ' -f1 > /workspace/output
    say output_sha256 "$("$bb" sha256sum /workspace/output | "$bb" cut -d' ' -f1)"
    # The undeclared sibling of the declared source must not be reachable.
    if [ -e "$3" ]; then say undeclared present; else say undeclared absent; fi
    say workspace_source_count "$("$bb" grep -c ' /workspace ' /proc/self/mountinfo)"
    ;;
  volume_write)
    printf '%s' "$owner" > /data/payload
    say payload_sha256 "$("$bb" sha256sum /data/payload | "$bb" cut -d' ' -f1)"
    ;;
  volume_read)
    say payload_sha256 "$("$bb" sha256sum /data/payload | "$bb" cut -d' ' -f1)"
    say payload_bytes "$("$bb" wc -c < /data/payload | "$bb" tr -d ' ')"
    ;;
  tmpfs_write)
    say scratch_fstype "$("$bb" stat -f -c %t /scratch)"
    printf '%s' "$owner" > /scratch/payload
    say payload_sha256 "$("$bb" sha256sum /scratch/payload | "$bb" cut -d' ' -f1)"
    ;;
  tmpfs_recreate)
    say scratch_fstype "$("$bb" stat -f -c %t /scratch)"
    if [ -e /scratch/payload ]; then say payload present; else say payload absent; fi
    ;;
  readonly)
    attempt root_write /vz-root-write
    attempt readonly_mount_write /ro/denied
    attempt declared_writable_mount_write /rw/allowed
    say readonly_source_sha256 "$("$bb" sha256sum /ro/input | "$bb" cut -d' ' -f1)"
    ;;
  ownership)
    attempt declared_writable_mount_write /rw/owned
    say created_uid "$("$bb" stat -c %u /rw/owned)"
    say created_gid "$("$bb" stat -c %g /rw/owned)"
    attempt forbidden_write /ro/forbidden
    ;;
  *) exit 64 ;;
esac
say end "$case_name"
