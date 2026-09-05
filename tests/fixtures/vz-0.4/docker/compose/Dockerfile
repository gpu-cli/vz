ARG FIXTURE_BASE
FROM ${FIXTURE_BASE}
ARG FIXTURE_BASE
RUN python3 -c 'import os,re; assert re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", os.environ["FIXTURE_BASE"])'
COPY service.py /fixture/service.py
ENTRYPOINT ["python3", "-u", "/fixture/service.py"]
