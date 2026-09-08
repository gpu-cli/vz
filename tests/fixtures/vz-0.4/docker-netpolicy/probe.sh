# vz docker-netpolicy probe: serve one fixed response, or report one fact.
# Runs as the container's PID 1 under `/bin/busybox sh -c` from the digest-pinned
# developer probe rootfs, so every applet is addressed through /bin/busybox. The
# caller selects the case in $1, the owner token in $2 and the role in $3. The
# serve case never exits on its own; the caller stops it.
set -eu
bb=/bin/busybox
case_name=$1
owner=$2
role=$3
port=8080
body="vznet|$owner|$role"
case "$case_name" in
  serve)
    "$bb" mkdir -p /www
    printf '%s' "$body" > /www/index.html
    # -f stays in the foreground so the container's PID 1 is the listener.
    exec "$bb" httpd -f -p "$port" -h /www
    ;;
  report)
    printf 'VZNET %s body=%s\n' "$owner" "$body"
    printf 'VZNET %s listeners=%s\n' "$owner" "$("$bb" netstat -tln 2>/dev/null | "$bb" grep -c ":$port ")"
    ;;
  *) exit 64 ;;
esac
