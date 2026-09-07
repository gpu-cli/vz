"""Source-fixed BusyBox provisioning primitives; this module never dispatches.

Use public exact-Machine Exec, --timeout 30, no TTY and default active stdin.
encode_payload returns PRIVATE bytes: do not persist, hash into receipts, print,
or put them in argv/environment. Setup/trust return only fixed_ack bytes so a
private recorder can preselect their entire output. After setup, run admit_script
without private stdin and parse its ADMIT acknowledgement for inode identities.
Scripts/plan/ack are public. The caller must
pin guest BusyBox, authenticate Machine/boot, retain complete/reaped command
receipts, and prove the exact registry container stopped/unmounted before cleanup.

Setup is create-only. A failure preserves partial state; it grants no repair or
cleanup permission. Completed setup records fixed-file identities and hashes in
a PRIVATE guest manifest. Cleanup prevalidates the whole inventory then removes
only named files and empty directories. Existing /etc/docker/certs.d parents are
never removed; newly created ones require recorded identity and emptiness.
Checks detect ordinary races/links, NOT malicious-root ABA/path substitution.
No recursive deletion, tar extraction, ambient TAR_OPTIONS, Python, or openssl.
"""
import base64
import hashlib
import re

import linux_docker_registry_fixture as fixture

FILES = ('ca.crt', 'server.crt', 'server.key', 'htpasswd', 'config.yml')
LIMIT = 65535
FILE_LIMIT = 16384


def require(value, code):
    if not value:
        raise ValueError('registry guest: ' + code)


def plan(owner, run_id, nonce):
    spec = fixture.resource_spec(owner, run_id)
    require(type(nonce) is str and re.fullmatch(
        r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', nonce), 'nonce')
    return {'schema_version': 1, 'owner': dict(owner), 'run_id': run_id, 'nonce': nonce,
            'directory': '/run/vz-registry-' + spec['labels']['com.vz.registry.owner'],
            'ca_directory': spec['guest_ca_directory'], 'authority': spec['authority'],
            'files': list(FILES), 'scope': 'exact_named_guest_files_not_hostile_root_containment'}


def checked(value):
    require(type(value) is dict, 'plan')
    try:
        require(value == plan(value['owner'], value['run_id'], value['nonce']), 'plan drift')
    except (KeyError, TypeError):
        raise ValueError('registry guest: malformed plan') from None
    return value


def encode_payload(selected, files, *, trust_ca=None):
    """PRIVATE bounded transport; all filenames/order are source selected."""
    selected = checked(selected)
    require(type(files) is dict and set(files) == set(FILES), 'file inventory')
    values = [files[name] for name in FILES] + [files['ca.crt'] if trust_ca is None else trust_ca]
    require(all(type(v) is bytes and 0 < len(v) <= FILE_LIMIT for v in values), 'file byte bounds')
    rows = [b'VZ_REGISTRY_PRIVATE_V1', selected['nonce'].encode()]
    for value in values:
        rows.extend([hashlib.sha256(value).hexdigest().encode(), base64.b64encode(value)])
    output = b'\n'.join(rows + [b'END', b''])
    require(len(output) <= LIMIT, 'payload byte bounds')
    return output


def parse_ack(raw, selected, *, action, expected=None):
    selected = checked(selected)
    require(action in ('ADMIT', 'INSPECT', 'CLEANUP'), 'action')
    require(type(raw) is bytes and len(raw) <= 256, 'ack bounds')
    try:
        rows = raw.decode('ascii').split('\n')
    except UnicodeError:
        raise ValueError('registry guest: ack encoding') from None
    require(len(rows) == 6 and rows[0] == 'VZ_REGISTRY_' + action + '_V1' and
            rows[1] == selected['nonce'] and rows[4:] == ['END', ''], 'ack framing')
    require(all(re.fullmatch(r'[1-9][0-9]{0,19}:[1-9][0-9]{0,19}', x) for x in rows[2:4]), 'inode identity')
    result = {'root_identity': rows[2], 'ca_identity': rows[3]}
    require(expected is None or result == expected, 'ack identity drift')
    return result


def fixed_ack(selected, *, action):
    selected = checked(selected)
    require(action in ('SETUP', 'TRUST'), 'private action')
    return ('vz-registry-' + action.lower() + ' ' + selected['nonce'] + '\n').encode('ascii')


def _common(selected):
    selected = checked(selected)
    return r'''set +x
set -eu
set -o pipefail
export LC_ALL=C
umask 077
bb=/bin/busybox
root=ROOT
ca=CA
nonce=NONCE
exec 3>&2 2>/dev/null
fail() { printf 'registry guest rejected\n' >&3; exit 1; }
ident() { "$bb" stat -c '%d:%i' "$1"; }
directory() {
 test ! -L "$1" && test -d "$1" || fail
 test "$("$bb" stat -c '%u:%g' "$1")" = 0:0 || fail
 case "$("$bb" stat -c '%a' "$1")" in 700|755|750|555) ;; *) fail ;; esac
}
regular() {
 test ! -L "$1" && test -f "$1" || fail
 test "$("$bb" stat -c '%u:%g:%a:%h' "$1")" = 0:0:600:1 || fail
 test "$("$bb" stat -c '%s' "$1")" -le "$2" || fail
}
fingerprint() {
 regular "$1" 16384
 before=$("$bb" stat -c '%d:%i:%s:%Y:%Z' "$1")
 digest=$("$bb" sha256sum "$1"); digest=${digest%% *}
 test "$before" = "$("$bb" stat -c '%d:%i:%s:%Y:%Z' "$1")" || fail
 printf '%s %s' "$before" "$digest"
}
parents() {
 test "$("$bb" id -u)" = 0 || fail
 for p in / /run /etc; do directory "$p"; done
}
ack() { printf 'VZ_REGISTRY_%s_V1\n%s\n%s\n%s\nEND\n' "$1" "$nonce" "$root_id" "$ca_id"; }
'''.replace('ROOT', selected['directory']).replace('CA', selected['ca_directory']).replace('NONCE', selected['nonce'])


def setup_script(selected):
    return _common(selected) + r'''
parents
test ! -e "$root" && test ! -L "$root" || fail
test ! -e "$ca" && test ! -L "$ca" || fail
"$bb" mkdir -m 700 "$root" || fail
root_id=$(ident "$root")
set -C
: > "$root/.parents"
for p in /etc/docker /etc/docker/certs.d; do
 created=0
 if test ! -e "$p" && test ! -L "$p"; then "$bb" mkdir -m 755 "$p" || fail; created=1; fi
 directory "$p"
 printf '%s %s\n' "$created" "$(ident "$p")" >> "$root/.parents"
done
"$bb" mkdir -m 700 "$ca" || fail
ca_id=$(ident "$ca")
"$bb" head -c 65536 > "$root/.payload" || fail
regular "$root/.payload" 65535
exec 4< "$root/.payload"
IFS= read -r header <&4 || fail
test "$header" = VZ_REGISTRY_PRIVATE_V1 || fail
IFS= read -r received <&4 || fail
test "$received" = "$nonce" || fail
: > "$root/.files"
for name in ca.crt server.crt server.key htpasswd config.yml trust.crt; do
 IFS= read -r hash <&4 || fail
 case "$hash" in *[!0-9a-f]*|'') fail ;; esac
 test "${#hash}" = 64 || fail
 IFS= read -r encoded <&4 || fail
 target="$root/$name"
 if test "$name" = trust.crt; then target="$ca/ca.crt"; fi
 printf '%s' "$encoded" | "$bb" base64 -d > "$target" || fail
 regular "$target" 16384
 test "$("$bb" stat -c '%s' "$target")" -gt 0 || fail
 test "$("$bb" base64 -w 0 "$target")" = "$encoded" || fail
 got=$("$bb" sha256sum "$target"); got=${got%% *}
 test "$got" = "$hash" || fail
 if test "$name" = trust.crt; then
  fingerprint "$target" > "$root/.trust"
 else
  printf '%s\n' "$(fingerprint "$target")" >> "$root/.files"
 fi
 "$bb" sync "$target" || fail
done
IFS= read -r end <&4 || fail
test "$end" = END || fail
if IFS= read -r extra <&4 || test -n "$extra"; then fail; fi
exec 4<&-
test "$(ident "$root")" = "$root_id" && test "$(ident "$ca")" = "$ca_id" || fail
printf '%s\n%s\n%s\n' "$nonce" "$root_id" "$ca_id" > "$root/.owner"
"$bb" rm "$root/.payload" || fail
"$bb" sync "$root/.owner" "$root/.files" "$root/.trust" "$root/.parents" "$root" "$ca" || fail
printf 'vz-registry-setup %s\n' "$nonce"
'''


def _owned(selected, identities):
    require(identities is None or (type(identities) is dict and set(identities) == {'root_identity', 'ca_identity'} and
            all(type(v) is str and re.fullmatch(r'[1-9][0-9]{0,19}:[1-9][0-9]{0,19}', v)
                for v in identities.values())), 'expected identities')
    root_id = '$(ident "$root")' if identities is None else identities['root_identity']
    ca_id = '$(ident "$ca")' if identities is None else identities['ca_identity']
    return _common(selected) + r'''
parents
for p in /etc/docker /etc/docker/certs.d "$root" "$ca"; do directory "$p"; done
test "$("$bb" stat -c '%a' "$root")" = 700 && test "$("$bb" stat -c '%a' "$ca")" = 700 || fail
root_id=ROOT_ID
ca_id=CA_ID
test "$(ident "$root")" = "$root_id" && test "$(ident "$ca")" = "$ca_id" || fail
regular "$root/.owner" 256
expected=$(printf '%s\n%s\n%s' "$nonce" "$root_id" "$ca_id")
test "$("$bb" cat "$root/.owner")" = "$expected" || fail
test "$("$bb" stat -c '%s' "$root/.owner")" = "$((${#expected}+1))" || fail
regular "$root/.parents" 256
regular "$root/.files" 2048
regular "$root/.trust" 256
# Bound captured inventory bytes before shell glob expansion. The suffix keeps
# trailing newlines from disappearing in command substitution. This is a
# quiescent-owned-tree check, not protection against malicious-root replacement.
for p in "$root" "$ca"; do
 inventory=$("$bb" find "$p" -mindepth 1 -maxdepth 1 -print | "$bb" head -c 4097 || exit 1; printf '.') || fail
 test "${#inventory}" -le 4097 || fail
done
# Check complete directory inventory, including hidden entries, without ls parsing.
count=0
for path in "$root"/* "$root"/.[!.]* "$root"/..?*; do
 if test ! -e "$path" && test ! -L "$path"; then continue; fi
 case "${path##*/}" in ca.crt|server.crt|server.key|htpasswd|config.yml|.owner|.parents|.files|.trust) ;; *) fail ;; esac
 count=$((count+1))
done
test "$count" = 9 || fail
count=0
for path in "$ca"/* "$ca"/.[!.]* "$ca"/..?*; do
 if test ! -e "$path" && test ! -L "$path"; then continue; fi
 test "${path##*/}" = ca.crt || fail
 count=$((count+1))
done
test "$count" = 1 || fail
exec 4< "$root/.files"
for name in ca.crt server.crt server.key htpasswd config.yml; do
 IFS= read -r expected <&4 || fail
 test "$(fingerprint "$root/$name")" = "$expected" || fail
done
if IFS= read -r extra <&4 || test -n "$extra"; then fail; fi
exec 4<&-
test "$(fingerprint "$ca/ca.crt")" = "$("$bb" cat "$root/.trust")" || fail
exec 4< "$root/.parents"
for p in /etc/docker /etc/docker/certs.d; do
 IFS=' ' read -r created recorded <&4 || fail
 case "$created" in 0|1) ;; *) fail ;; esac
 test "$(ident "$p")" = "$recorded" || fail
done
if IFS= read -r extra <&4 || test -n "$extra"; then fail; fi
exec 4<&-
test "$(ident "$root")" = "$root_id" && test "$(ident "$ca")" = "$ca_id" || fail
'''.replace('ROOT_ID', root_id).replace('CA_ID', ca_id)


def admit_script(selected):
    """Public no-stdin admission after positively acknowledged exclusive setup.

    Identities are observed here, not independently authenticated by this script;
    the caller binds the exact Machine, setup receipt and public nonce.
    """
    return _owned(selected, None) + '\nack ADMIT\n'


def inspect_script(selected, identities):
    return _owned(selected, identities) + '\nack INSPECT\n'


def install_trust_script(selected, identities, previous_ca_sha256, ca_sha256):
    """Private stdin is the new public CA PEM; no server secret is changed."""
    for value in (previous_ca_sha256, ca_sha256):
        require(type(value) is str and re.fullmatch(r'[0-9a-f]{64}', value), 'public CA digest')
    require(previous_ca_sha256 != ca_sha256, 'trust phase must change')
    return _owned(selected, identities) + r'''
got=$("$bb" sha256sum "$ca/ca.crt"); got=${got%% *}
test "$got" = OLD_HASH || fail
set -C
"$bb" head -c 16385 > "$root/.new-ca" || fail
regular "$root/.new-ca" 16384
test "$("$bb" stat -c '%s' "$root/.new-ca")" -gt 0 || fail
got=$("$bb" sha256sum "$root/.new-ca"); got=${got%% *}
test "$got" = NEW_HASH || fail
new_ca_before=$(fingerprint "$root/.new-ca")
trust_id=$(ident "$ca/ca.crt")
# Only this explicit public-trust phase may overwrite this exact owned inode.
"$bb" cat "$root/.new-ca" >| "$ca/ca.crt" || fail
test "$(ident "$ca/ca.crt")" = "$trust_id" || fail
test "$(fingerprint "$root/.new-ca")" = "$new_ca_before" || fail
got=$("$bb" sha256sum "$ca/ca.crt"); got=${got%% *}
test "$got" = NEW_HASH || fail
fingerprint "$ca/ca.crt" >| "$root/.trust"
"$bb" sync "$ca/ca.crt" "$root/.trust" || fail
"$bb" rm "$root/.new-ca" || fail
"$bb" sync "$root" || fail
printf 'vz-registry-trust %s\n' "$nonce"
'''.replace('OLD_HASH', previous_ca_sha256).replace('NEW_HASH', ca_sha256)


def cleanup_script(selected, identities):
    """Caller must first prove exact registry down and mounts released."""
    return _owned(selected, identities) + r'''
exec 4< "$root/.parents"
IFS=' ' read -r docker_created docker_id <&4 || fail
IFS=' ' read -r certs_created certs_id <&4 || fail
exec 4<&-
test "$(fingerprint "$ca/ca.crt")" = "$("$bb" cat "$root/.trust")" || fail
"$bb" rm "$ca/ca.crt" || fail
test "$(ident "$ca")" = "$ca_id" || fail
"$bb" rmdir "$ca" || fail
exec 4< "$root/.files"
for name in ca.crt server.crt server.key htpasswd config.yml; do
 IFS= read -r expected <&4 || fail
 test "$(ident "$root")" = "$root_id" && test "$(fingerprint "$root/$name")" = "$expected" || fail
 "$bb" rm "$root/$name" || fail
done
exec 4<&-
for name in .files .trust .parents .owner; do
 regular "$root/$name" 2048
 "$bb" rm "$root/$name" || fail
done
test "$(ident "$root")" = "$root_id" || fail
"$bb" rmdir "$root" || fail
if test "$certs_created" = 1; then
 test "$(ident /etc/docker/certs.d)" = "$certs_id" || fail
 "$bb" rmdir /etc/docker/certs.d || fail
fi
if test "$docker_created" = 1; then
 test "$(ident /etc/docker)" = "$docker_id" || fail
 "$bb" rmdir /etc/docker || fail
fi
test ! -e "$root" && test ! -L "$root" && test ! -e "$ca" && test ! -L "$ca" || fail
"$bb" sync /run /etc || fail
ack CLEANUP
'''
