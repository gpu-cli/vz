"""Private container loopback HTTP service and bounded, no-retry observer."""
import http.client
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import sys
import time

MARKER = Path('/tmp/vz-parallel-health-marker')
READY = Path('/tmp/vz-parallel-health-ready')
PORT = 8080


def emit(row):
    print(json.dumps(row, sort_keys=True, separators=(',', ':')), flush=True)


def serve():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != '/health':
                self.send_error(404)
                return
            body = MARKER.read_bytes()
            if not 1 <= len(body) <= 256:
                raise ValueError('invalid host-written marker')
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = HTTPServer(('127.0.0.1', PORT), Handler)
    with READY.open('x') as ready:
        ready.write('ready\n')
    server.serve_forever()


def probe(token, timing):
    start = time.monotonic_ns()
    emit({'type': 'start', 'schema_version': 1, 'token': token, 'pid': os.getpid(),
          'unix_ns': time.time_ns(), 'monotonic_ns': start, 'timing': timing})
    for sequence in range(timing['samples']):
        planned = start + sequence * timing['interval_ns']
        remaining = planned - time.monotonic_ns()
        if remaining > 0:
            time.sleep(remaining / 10**9)
        began = time.monotonic_ns()
        wall = time.time_ns()
        if not 0 <= began - planned <= timing['max_lateness_ns']:
            raise ValueError('health sample missed its scheduled deadline')
        connection = http.client.HTTPConnection('127.0.0.1', PORT,
                                                timeout=timing['request_timeout_ns'] / 10**9)
        try:
            connection.request('GET', '/health', headers={'Connection': 'close'})
            response = connection.getresponse()
            body = response.read(257)
            status = response.status
        finally:
            connection.close()
        ended, finished_wall = time.monotonic_ns(), time.time_ns()
        if status != 200 or body != (token + '\n').encode():
            raise ValueError('HTTP health response differs from host-written marker')
        if not 0 <= ended - began <= timing['request_timeout_ns']:
            raise ValueError('health request exceeded its deadline')
        emit({'type': 'sample', 'sequence': sequence, 'planned_monotonic_ns': planned,
              'started_monotonic_ns': began, 'finished_monotonic_ns': ended,
              'started_unix_ns': wall, 'finished_unix_ns': finished_wall,
              'status': status, 'body': body.decode()})
    emit({'type': 'end', 'samples': timing['samples'], 'monotonic_ns': time.monotonic_ns(),
          'unix_ns': time.time_ns()})


def main():
    if sys.argv[1] == 'serve' and len(sys.argv) == 2:
        serve()
    elif sys.argv[1] == 'probe' and len(sys.argv) == 4:
        probe(sys.argv[2], json.loads(sys.argv[3]))
    else:
        raise ValueError('unknown health fixture operation')


if __name__ == '__main__':
    main()
