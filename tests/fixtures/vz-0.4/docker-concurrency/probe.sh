# VZ concurrency probe. Reports what it observed; the host decides outcomes.
# Invoked as: /bin/busybox sh -c "<this file>" vzconc <case> <arguments...>
set -eu
BB=/bin/busybox
READY=/run/vz-concurrency-ready
RENDEZVOUS=/vz-rendezvous
arrived() { "$BB" ls "$RENDEZVOUS" | "$BB" wc -l | "$BB" tr -d ' '; }
case "${1:-}" in
ready)
	# $2 container marker, $3 idle seconds
	printf 'VZREADY %s\n' "$2" > "$READY"
	exec "$BB" sleep "$3"
	;;
report)
	# $2 container marker, $3 exec slot, $4 participants, $5 poll seconds
	printf 'VZSLOT %s %s\n' "$2" "$3" > "$RENDEZVOUS/slot-$3"
	waited=0
	count=$(arrived)
	while [ "$count" -lt "$4" ]; do
		[ "$waited" -lt "$5" ] || exit 65
		"$BB" sleep 1
		waited=$((waited + 1))
		count=$(arrived)
	done
	"$BB" cat "$READY"
	printf 'VZEXEC %s %s %s %s\n' "$2" "$3" "$count" "$waited"
	;;
*)
	exit 64
	;;
esac
