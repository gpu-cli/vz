#!/usr/bin/env python3
"""Kernel-released build lock retained by the child across caller cancellation."""
import fcntl
import os
import stat
import subprocess
import sys


if __name__ == "__main__":
    descriptor = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600:
            raise SystemExit("unsafe youki build lock file")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SystemExit("another youki builder owns the live advisory lock") from error
        environment = dict(os.environ, VZ_YOUKI_BUILD_LOCK_FD=str(descriptor))
        raise SystemExit(subprocess.run(sys.argv[2:], env=environment, pass_fds=(descriptor,), check=False).returncode)
    finally:
        os.close(descriptor)
