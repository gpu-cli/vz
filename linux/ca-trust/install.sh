# Sourced from the verified Developer initramfs before the guest agent starts.
# No environment-selected paths or host trust. The initramfs digest binds this
# script, its expected digest and the exact public CA bytes together.
verify_developer_ca_trust() {
  ca_origin=/
  ca_source="$ca_origin/etc/vz/ca-certificates.crt"
  ca_digest="$ca_origin/etc/vz/ca-trust.sha256"
  for ca_path in "$ca_origin/etc" "$ca_origin/etc/vz"; do
    [ -d "$ca_path" ] && [ ! -L "$ca_path" ] || return 1
  done
  for ca_path in "$ca_source" "$ca_digest"; do
    [ -f "$ca_path" ] && [ ! -L "$ca_path" ] || return 1
    [ "$(/bin/busybox stat -c '%h' "$ca_path")" = 1 ] || return 1
  done
  ca_expected=$(/bin/busybox cat "$ca_digest") || return 1
  [ "${#ca_expected}" = 64 ] || return 1
  case "$ca_expected" in *[!0-9a-f]*) return 1 ;; esac
  ca_observed=$(/bin/busybox sha256sum "$ca_source") || return 1
  [ "${ca_observed%% *}" = "$ca_expected" ] || return 1
  ca_empty="$ca_origin/etc/vz/empty-ca-directory"
  [ -d "$ca_empty" ] && [ ! -L "$ca_empty" ] || return 1
  ca_children=$(/bin/busybox ls -A "$ca_empty") || return 1
  [ -z "$ca_children" ] || return 1
}

install_developer_ca_trust() {
  ca_root="$1"
  verify_developer_ca_trust || return 1
  [ -d "$ca_root" ] && [ ! -L "$ca_root" ] || return 1
  # The control-plane namespace must not adopt or traverse distro /etc/ssl.
  # An external image may legitimately carry different CA bytes or symlinks.
  for ca_path in "$ca_root/etc" "$ca_root/etc/vz"; do
    [ ! -L "$ca_path" ] || return 1
    if [ -e "$ca_path" ]; then [ -d "$ca_path" ] || return 1; fi
    /bin/busybox mkdir -p "$ca_path" || return 1
  done
  for ca_name in etc/vz/ca-certificates.crt etc/vz/ca-trust.sha256; do
    ca_target="$ca_root/$ca_name"
    [ ! -L "$ca_target" ] || return 1
    if [ -e "$ca_target" ]; then
      [ -f "$ca_target" ] && [ "$(/bin/busybox stat -c '%h' "$ca_target")" = 1 ] || return 1
      /bin/busybox cmp "$ca_origin/$ca_name" "$ca_target" || return 1
    else
      /bin/busybox cp "$ca_origin/$ca_name" "$ca_target" || return 1
    fi
    /bin/busybox chmod 444 "$ca_target" || return 1
  done
  ca_empty="$ca_root/etc/vz/empty-ca-directory"
  [ ! -L "$ca_empty" ] || return 1
  if [ -e "$ca_empty" ]; then [ -d "$ca_empty" ] || return 1; fi
  /bin/busybox mkdir -p "$ca_empty" || return 1
  ca_children=$(/bin/busybox ls -A "$ca_empty") || return 1
  [ -z "$ca_children" ] || return 1
  /bin/busybox chmod 555 "$ca_empty" || return 1
  ca_observed=$(/bin/busybox sha256sum "$ca_root/etc/vz/ca-certificates.crt") || return 1
  [ "${ca_observed%% *}" = "$ca_expected" ] || return 1
}
