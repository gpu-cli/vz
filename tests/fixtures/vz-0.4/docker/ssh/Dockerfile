ARG FIXTURE_SSH_BASE
FROM ${FIXTURE_SSH_BASE}
ARG FIXTURE_SSH_BASE
RUN python3 -c 'import os,re; assert re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", os.environ["FIXTURE_SSH_BASE"])'
RUN mkdir -p /run/sshd /run/secrets
COPY sshd_config response.txt /fixture/
ENTRYPOINT ["/usr/sbin/sshd", "-D", "-e", "-f", "/fixture/sshd_config"]
