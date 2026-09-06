"""Fixed-command private SSH server; account preparation is separately admitted."""
import grp
import os
from pathlib import Path
import pwd
import re
import stat
import sys

from ssh_probe import read_regular, require

DIRECTORY = Path('/run/vz-ssh-server')
ACCOUNT_IDS = {'vzssh': 10001, 'sshd': 10002}


def install_accounts(root=Path('/')):
    """Fresh image only: append new accounts; never repair or unlock existing ones.

    Debian's Linux OpenSSH 9.2p1 platform_locked_account uses prefix '!'. '*'
    cannot authenticate a Unix password but is not that locked-account prefix.
    Empty expiry fields avoid introducing an expired account. Authentication is
    additionally restricted to publickey with PAM/password/interactive disabled.
    """
    additions = {
        'passwd': b'vzssh:x:10001:10001:vz SSH fixture:/var/empty/vzssh:/bin/sh\n'
                  b'sshd:x:10002:10002:SSH privilege separation:/run/sshd:/usr/sbin/nologin\n',
        'group': b'vzssh:x:10001:\nsshd:x:10002:\n',
        'shadow': b'vzssh:*:::::::\nsshd:*:::::::\n',
        'gshadow': b'vzssh:!::\nsshd:!::\n',
    }
    opened = []
    try:
        # Open and validate every target before appending anything; use the same
        # descriptors, preserve existing rows/owners/modes, reject ID collisions.
        for name, suffix in additions.items():
            path = root / 'etc' / name
            data = read_regular(path, 65536)
            require(data.endswith(b'\n'))
            lines = data.decode().splitlines()
            require(all(line.split(':', 1)[0] not in ACCOUNT_IDS for line in lines))
            if name in ('passwd', 'group'):
                require(all(line.split(':')[2] not in ('10001', '10002') for line in lines))
            fd = os.open(path, os.O_RDWR | os.O_APPEND | os.O_NOFOLLOW | os.O_NONBLOCK)
            opened.append((fd, suffix))
            info = os.fstat(fd)
            require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size == len(data))
            require(os.read(fd, len(data) + 1) == data)
        for fd, suffix in opened:
            require(os.write(fd, suffix) == len(suffix))
    finally:
        for fd, _ in opened:
            os.close(fd)


def accounts():
    login = pwd.getpwnam('vzssh')
    separation = pwd.getpwnam('sshd')
    require(login.pw_uid == login.pw_gid == ACCOUNT_IDS['vzssh'] and
            separation.pw_uid == separation.pw_gid == ACCOUNT_IDS['sshd'] and login.pw_shell == '/bin/sh' and
            login.pw_dir == '/var/empty/vzssh' and separation.pw_dir == '/run/sshd' and
            separation.pw_shell == '/usr/sbin/nologin')
    require(grp.getgrnam('vzssh').gr_gid == login.pw_gid and
            grp.getgrnam('sshd').gr_gid == separation.pw_gid)
    shadow = read_regular('/etc/shadow', 65536).decode().splitlines()
    for name in ACCOUNT_IDS:
        require([line for line in shadow if line.startswith(name + ':')] == [name + ':*:::::::'])
    return login, separation


def prepare():
    require(os.getuid() == 0)
    install_accounts()
    accounts()
    for name in ('/run/sshd', str(DIRECTORY), '/var/empty/vzssh'):
        path = Path(name)
        path.mkdir(mode=0o755, parents=True, exist_ok=True)
        info = path.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and
                stat.S_IMODE(info.st_mode) == 0o755)


def public_response():
    value = read_regular(DIRECTORY / 'response.txt', 128)
    require(re.fullmatch(rb'vz-ssh-response:vzssh-[0-9a-f]{24}\n', value))
    return value


def serve():
    require(os.getuid() == 0)
    accounts()
    directory = DIRECTORY.lstat()
    require(stat.S_ISDIR(directory.st_mode) and directory.st_uid == 0 and
            stat.S_IMODE(directory.st_mode) == 0o755)
    for name, mode in (('host_key', 0o600), ('authorized_keys', 0o444), ('response.txt', 0o444)):
        info = (DIRECTORY / name).lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                info.st_uid == 0 and stat.S_IMODE(info.st_mode) == mode)
    public_response()
    require(not os.path.lexists(DIRECTORY / 'sshd.pid'))
    os.execve('/usr/sbin/sshd', ['/usr/sbin/sshd', '-D', '-e', '-f', '/fixture/sshd_config'],
              {'PATH': '/usr/local/bin:/usr/bin:/bin', 'LC_ALL': 'C'})


def main():
    try:
        require(len(sys.argv) == 2)
        if sys.argv[1] == 'prepare':
            prepare()
        elif sys.argv[1] == 'serve':
            serve()
        elif sys.argv[1] == 'response':
            require(os.getuid() == pwd.getpwnam('vzssh').pw_uid and
                    os.environ.get('SSH_ORIGINAL_COMMAND') == 'vz-public-response')
            sys.stdout.buffer.write(public_response())
            sys.stdout.buffer.flush()
        else:
            require(False)
        return 0
    except Exception:
        print('VZ_SSH_SERVER_CONTRACT_REJECTED', file=sys.stderr, flush=True)
        return 70


if __name__ == '__main__':
    raise SystemExit(main())
