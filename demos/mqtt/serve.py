#!/usr/bin/env python3
"""Serve the browser demo and proxy IoT publishes; Python standard library only."""
import argparse
import json
import urllib.error
import urllib.parse
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--port', type=int, default=5072)
parser.add_argument('--roto-port', type=int, default=5071)
args = parser.parse_args()


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *params, **kwargs):
        super().__init__(*params, directory=str(Path(__file__).parent), **kwargs)

    def do_GET(self):
        if self.path == '/config':
            body = json.dumps({'mqttUrl': f'ws://127.0.0.1:{args.roto_port}/mqtt'}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(body)
        else:
            super().do_GET()

    def do_POST(self):
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path != '/publish':
            self.send_error(404)
            return
        topic = urllib.parse.parse_qs(parsed.query).get('topic', [''])[0]
        size = int(self.headers.get('Content-Length', '0'))
        if size > 1024 * 1024:
            self.send_error(413)
            return
        request = urllib.request.Request(
            f'http://127.0.0.1:{args.roto_port}/topics/{urllib.parse.quote(topic, safe="")}?qos=1',
            data=self.rfile.read(size), method='POST',
            headers={'Host': f'iotdata.localhost:{args.roto_port}'},
        )
        try:
            with urllib.request.urlopen(request, timeout=3) as response:
                status, body = response.status, response.read()
        except urllib.error.HTTPError as error:
            status, body = error.code, error.read()
        except urllib.error.URLError as error:
            status, body = 502, str(error).encode()
        self.send_response(status)
        self.end_headers()
        self.wfile.write(body)


print(f'Open http://127.0.0.1:{args.port}/', flush=True)
try:
    ThreadingHTTPServer(('127.0.0.1', args.port), Handler).serve_forever()
except KeyboardInterrupt:
    pass
