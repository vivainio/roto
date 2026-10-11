#!/usr/bin/env python3
"""Exercise browser S3 routes and API separation against a disposable seeded server."""
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parent.parent


def smoke():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    endpoint = f'http://127.0.0.1:{port}'
    with tempfile.TemporaryFile(mode='w+') as log:
        proc = subprocess.Popen([str(ROOT / 'target/debug/roto-server'), '--ephemeral', '--port', str(port),
                                 '--setup', str(ROOT / 'examples/demo/setup.lua')], stdout=log, stderr=log)
        def request(path, method='GET', data=None, headers=None, follow=True):
            req = urllib.request.Request(endpoint + path, data=data, method=method, headers=headers or {})
            try:
                if follow:
                    response = urllib.request.urlopen(req, timeout=5)
                else:
                    class NoRedirect(urllib.request.HTTPRedirectHandler):
                        def redirect_request(self, req, fp, code, msg, headers, newurl):
                            return None
                    response = urllib.request.build_opener(NoRedirect).open(req, timeout=5)
            except urllib.error.HTTPError as e:
                response = e
            with response:
                return response.status, response.headers, response.read(), response.url
        api = {'Authorization': 'AWS4-HMAC-SHA256 Credential=test/20261011/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=fake'}
        def put(key, body, **headers):
            assert request('/browser-test/' + key, 'PUT', body.encode(), api | {'Content-Type': 'application/octet-stream'} | headers)[0] == 200
        try:
            deadline = time.monotonic() + 15
            while True:
                try:
                    if request('/roto-api/health')[0] == 200:
                        break
                except OSError:
                    pass
                if time.monotonic() > deadline or proc.poll() is not None:
                    log.seek(0)
                    raise AssertionError(log.read())
                time.sleep(.05)
            assert request('/browser-test', 'PUT', b'', api)[0] == 200
            put('foo.html', '<a href="assets/style.css">style</a>', **{'Content-Type': 'application/octet-stream'})
            put('assets/style.css', 'body { color: red; }')
            put('nested/index.html', '<h1>Nested</h1>')
            put('notes/a%20%2B%20space%20%E2%98%95.txt', 'hello')
            status, headers, body, _ = request('/s3/browser-test/foo.html')
            assert status == 200 and headers['Content-Type'].startswith('text/html')
            assert b'href="assets/style.css"' in body and 'Content-Disposition' not in headers
            assert request('/s3/browser-test/assets/style.css')[1]['Content-Type'].startswith('text/css')
            status, _, body, url = request('/s3/browser-test')
            assert status == 200 and url.endswith('/s3/browser-test/')
            assert b'foo.html' in body and b'assets/' in body
            assert request('/s3/browser-test/nested')[3].endswith('/nested/')
            assert request('/s3/browser-test/nested/')[2] == b'<h1>Nested</h1>'
            listing = request('/s3/browser-test/notes/')[2]
            assert b'a%20%2B%20space%20%E2%98%95.txt' in listing
            assert request('/s3/browser-test/notes/a%20%2B%20space%20%E2%98%95.txt')[2] == b'hello'
            assert request('/s3/browser-test/missing')[0] == 404
            assert request('/s3/missing-bucket/')[0] == 404
            assert request('/s3/browser-test/foo.html', 'POST', b'no')[0] == 405
            assert request('/s3/browser-test/foo.html', 'HEAD')[2] == b''
            assert request('/s3/browser-test/foo.html', headers={'Range': 'bytes=0-2'})[2] == b'<a '
            etag = headers['ETag']
            assert request('/s3/browser-test/foo.html', headers={'If-None-Match': etag})[0] == 304
            website = {'Host': f'browser-test.s3-website.example.test:{port}'}
            assert request('/foo.html', headers=website)[2].startswith(b'<a ')
            put('roto-api/health', '<h1>Bucket health page</h1>')
            assert request('/roto-api/health', headers=website)[2] == b'<h1>Bucket health page</h1>'
            put('roto-api/s3/notifications', 'bucket notifications object')
            assert request('/roto-api/s3/notifications', headers=website)[2] == b'bucket notifications object'
            inspection = request('/roto-api/s3/object?bucket=browser-test&key=foo.html')
            assert inspection[1]['Content-Disposition'] == 'attachment'
            config = '<WebsiteConfiguration><IndexDocument><Suffix>home.html</Suffix></IndexDocument><ErrorDocument><Key>error.html</Key></ErrorDocument></WebsiteConfiguration>'
            put('home.html', '<h1>Home</h1>')
            put('error.html', '<h1>Missing</h1>')
            assert request('/browser-test?website', 'PUT', config.encode(), api)[0] == 200
            assert request('/s3/browser-test/')[2] == b'<h1>Home</h1>'
            assert request('/s3/browser-test/missing')[0:3:2] == (404, b'<h1>Missing</h1>')
            put('redirect', '', **{'x-amz-website-redirect-location': '/foo.html'})
            assert request('/s3/browser-test/redirect')[3].endswith('/s3/browser-test/foo.html')
            rules = '<WebsiteConfiguration><RoutingRules><RoutingRule><Condition><KeyPrefixEquals>old/</KeyPrefixEquals></Condition><Redirect><ReplaceKeyWith>home.html</ReplaceKeyWith><HttpRedirectCode>307</HttpRedirectCode></Redirect></RoutingRule></RoutingRules></WebsiteConfiguration>'
            assert request('/browser-test?website', 'PUT', rules.encode(), api)[0] == 200
            redirected = request('/s3/browser-test/old/page.html', follow=False)
            assert redirected[0] == 307 and redirected[1]['Location'] == '/s3/browser-test/home.html'
            whole = '<WebsiteConfiguration><RedirectAllRequestsTo><HostName>example.test</HostName><Protocol>https</Protocol></RedirectAllRequestsTo></WebsiteConfiguration>'
            assert request('/browser-test?website', 'PUT', whole.encode(), api)[0] == 200
            redirected = request('/s3/browser-test/foo.html', follow=False)
            assert redirected[0] == 302 and redirected[1]['Location'] == 'https://example.test/foo.html'
            assert request('/browser-test?website', 'DELETE', headers=api)[0] == 204
            for i in range(101):
                put(f'bulk/item-{i:03}.txt', str(i))
            listing = request('/s3/browser-test/bulk/')[2].decode()
            import re
            next_page = re.search(r'href="(\?continuation=[^"]+)"', listing).group(1)
            assert 'item-099.txt' in listing and 'item-100.txt' not in listing
            assert b'item-100.txt' in request('/s3/browser-test/bulk/' + next_page)[2]
            put('escaped/%3Cscript%3E%26%22.txt', 'safe')
            escaped = request('/s3/browser-test/escaped/')[2]
            assert b'<script>' not in escaped and b'&lt;script&gt;&amp;&quot;' in escaped
            # Signed API requests do not become browser routes.
            assert request('/browser-test/foo.html', headers=api)[2].startswith(b'<a ')
            assert request('/s3/browser-test/foo.html', headers=api)[0] == 404
            # Deleted latest versions disappear from the browser.
            assert request('/browser-test/foo.html', 'DELETE', headers=api)[0] == 204
            assert request('/s3/browser-test/foo.html')[0] == 404
            print('S3 browser smoke passed: HTML, assets, folders, encoded keys, host routing, configuration, API separation')
        finally:
            proc.terminate()
            proc.wait(timeout=10)


if __name__ == '__main__':
    smoke()
