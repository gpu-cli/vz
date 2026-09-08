# Retained SSH-input provenance

`linux_docker_ssh_acquire` fetches every Debian input the pin gives a
`repository_path`: the Release, the package index, the eight packages and the
source archives, about 23 MB, none of it in this tree.

These nine files are the ones the pin gives no path, because they did not come
from the archive. They are records of the admission that produced the pin — the
keyring recovered from the base image, two files read out of it, the retained
`gpgv` output, the closure and ELF proofs, and one stanza excerpted from
`Sources.xz`. There is no generator for them in this repository, so they are
retained rather than reproduced, which is the second form GOAL-0.4.0.md admits:
"pinned by immutable digest **or** retained in a content-addressed replayable
fixture".

Every file here is content-addressed: `linux_docker_ssh_input.verify` checks each
one against the `sha256` and `size` already in
`config/docker-ssh-packages-bookworm-arm64.json`, so a corrupted or substituted
byte fails admission rather than reaching a run.

Three could later stop being retained, and should be: the keyring is extractable
from the pinned base image layer, which already carries its `layer_digest`,
`diff_id` and `tar_path`; `openssh.source-stanza` is an excerpt of the
`Sources.xz` that is fetched; and `base-var--lib--dpkg--status` and
`base-usr--lib--os-release` are readable from the same base layer once the pin
states their paths.
