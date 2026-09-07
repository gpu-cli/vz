# vz docker-limits allocator: grow anonymous shell memory past 1 GiB.
# Runs as the container's PID 1 under `/bin/busybox sh -c` from the digest-pinned
# developer probe rootfs (applets are addressed through /bin/busybox). Each loop
# doubles one 1 MiB string; the tenth doubling needs more than the 1 GiB limit,
# so the kernel OOM killer must terminate this process (exit 137). Surviving all
# eleven doublings (2 GiB) exits 61: the limit was not enforced. Never run it
# without a memory limit.
set -eu
bb=/bin/busybox
chunk=$("$bb" head -c 1048576 /dev/zero | "$bb" tr '\0' 'x')
[ "${#chunk}" -eq 1048576 ] || exit 62
buffer=$chunk
step=0
printf 'VZ_ALLOC step=0 bytes=%s\n' "${#buffer}"
while [ "$step" -lt 11 ]; do
  buffer="$buffer$buffer"
  step=$((step + 1))
  printf 'VZ_ALLOC step=%s bytes=%s\n' "$step" "${#buffer}"
done
exit 61
