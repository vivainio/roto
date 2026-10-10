#!/usr/bin/env python3
"""Native end-to-end checks for IoT topic sockets and API Gateway callbacks.

Run after ``cargo build -p roto-server --locked``. Uses only Python's standard
library; it does not start Moto or rely on AWS SDK packages.
"""
import base64
import json
import secrets
import socket
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


class WebSocket:
    def __init__(self, host, port, path):
        self.sock = socket.create_connection((host, port), timeout=3)
        self.sock.settimeout(3)
        self.buffer = bytearray()
        key = base64.b64encode(secrets.token_bytes(16)).decode()
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        )
        self.sock.sendall(request.encode("ascii"))
        response = self.read_until(b"\r\n\r\n").decode("latin-1")
        assert response.startswith("HTTP/1.1 101 "), response

    def read_until(self, marker):
        while marker not in self.buffer:
            data = self.sock.recv(4096)
            if not data:
                raise AssertionError("WebSocket closed during handshake")
            self.buffer.extend(data)
        end = self.buffer.index(marker) + len(marker)
        result = bytes(self.buffer[:end])
        del self.buffer[:end]
        return result

    def read_exact(self, count):
        while len(self.buffer) < count:
            data = self.sock.recv(max(4096, count - len(self.buffer)))
            if not data:
                raise AssertionError("WebSocket closed while reading a frame")
            self.buffer.extend(data)
        result = bytes(self.buffer[:count])
        del self.buffer[:count]
        return result

    def receive(self, timeout=3):
        self.sock.settimeout(timeout)
        try:
            first, second = self.read_exact(2)
            opcode = first & 0x0F
            length = second & 0x7F
            if length == 126:
                length = struct.unpack("!H", self.read_exact(2))[0]
            elif length == 127:
                length = struct.unpack("!Q", self.read_exact(8))[0]
            mask = self.read_exact(4) if second & 0x80 else None
            data = self.read_exact(length)
            if mask:
                data = bytes(value ^ mask[index % 4] for index, value in enumerate(data))
            if opcode == 0x9:
                self.send_frame(0xA, data)
                return self.receive(timeout)
            if opcode == 0x8:
                return (opcode, data)
            return opcode, data
        finally:
            self.sock.settimeout(3)

    def receive_json(self, timeout=3):
        opcode, data = self.receive(timeout)
        assert opcode == 0x1, (opcode, data)
        return json.loads(data)

    def send_frame(self, opcode, payload):
        mask = secrets.token_bytes(4)
        size = len(payload)
        if size < 126:
            header = bytes((0x80 | opcode, 0x80 | size))
        elif size < 65536:
            header = bytes((0x80 | opcode, 0x80 | 126)) + struct.pack("!H", size)
        else:
            header = bytes((0x80 | opcode, 0x80 | 127)) + struct.pack("!Q", size)
        masked = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
        self.sock.sendall(header + mask + masked)

    def send_json(self, value):
        self.send_frame(0x1, json.dumps(value).encode())

    def close(self):
        try:
            self.send_frame(0x8, b"")
        except OSError:
            pass
        self.sock.close()


def http_request(endpoint, port, service, path, data, expected_status=200):
    request = urllib.request.Request(
        endpoint + path,
        data=data,
        method="POST",
        headers={"Host": f"{service}.localhost:{port}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            status, body = response.status, response.read()
    except urllib.error.HTTPError as error:
        status, body = error.code, error.read()
    assert status == expected_status, (path, status, body)
    return body


def main():
    root = Path(__file__).resolve().parent.parent
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}"

    with tempfile.TemporaryDirectory(prefix="roto-websocket-smoke-") as directory:
        log_path = Path(directory) / "server.log"
        with log_path.open("w+") as log:
            process = subprocess.Popen(
                [str(root / "target/debug/roto-server"), "--ephemeral", "--port", str(port)],
                stdout=log,
                stderr=log,
            )
            clients = []
            try:
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        log.seek(0)
                        raise RuntimeError(log.read())
                    try:
                        urllib.request.urlopen(endpoint + "/roto-api/health", timeout=1).close()
                        break
                    except OSError:
                        time.sleep(0.05)
                else:
                    log.seek(0)
                    raise RuntimeError("WebSocket smoke server did not become healthy\n" + log.read())

                first = WebSocket("127.0.0.1", port, "/roto-api/ws")
                second = WebSocket("127.0.0.1", port, "/roto-api/iot/ws")
                clients.extend((first, second))
                ready_first = first.receive_json()
                ready_second = second.receive_json()
                assert ready_first["type"] == ready_second["type"] == "ready"
                assert ready_first["connectionId"] != ready_second["connectionId"]

                first.send_json({"action": "subscribe", "topic": "sensors/+/temperature"})
                assert first.receive_json() == {
                    "type": "subscribed", "topic": "sensors/+/temperature"
                }
                topic = urllib.parse.quote("sensors/kitchen/temperature", safe="/")
                http_request(endpoint, port, "iotdata", f"/topics/{topic}?qos=1", b"warm")
                event = first.receive_json()
                assert event == {
                    "type": "message",
                    "topic": "sensors/kitchen/temperature",
                    "qos": 1,
                    "payload": {"encoding": "utf-8", "data": "warm"},
                }
                try:
                    second.receive_json(timeout=0.15)
                except socket.timeout:
                    pass
                else:
                    raise AssertionError("an unsubscribed socket received an IoT publish")

                first.send_json({"action": "unsubscribe", "topic": "sensors/+/temperature"})
                assert first.receive_json() == {
                    "type": "unsubscribed", "topic": "sensors/+/temperature"
                }
                http_request(endpoint, port, "iotdata", f"/topics/{topic}", b"ignored")
                try:
                    first.receive_json(timeout=0.15)
                except socket.timeout:
                    pass
                else:
                    raise AssertionError("an unsubscribed socket received a later publish")

                first.send_json({"action": "subscribe", "topic": "alerts/#"})
                assert first.receive_json() == {"type": "subscribed", "topic": "alerts/#"}
                binary_topic = urllib.parse.quote("alerts/system/start", safe="/")
                http_request(endpoint, port, "iotdata", f"/topics/{binary_topic}", b"\x00\xff")
                assert first.receive_json() == {
                    "type": "message",
                    "topic": "alerts/system/start",
                    "qos": 0,
                    "payload": {"encoding": "base64", "data": "AP8="},
                }

                connection_id = ready_first["connectionId"]
                connection_path = "/@connections/" + urllib.parse.quote(connection_id, safe="")
                http_request(
                    endpoint,
                    port,
                    "execute-api",
                    connection_path,
                    b'{"from":"management-api"}',
                )
                assert first.receive_json() == {"from": "management-api"}
                http_request(
                    endpoint,
                    port,
                    "execute-api",
                    "/@connections/" + urllib.parse.quote(ready_second["connectionId"], safe=""),
                    b"\x00\xff",
                )
                opcode, payload = second.receive()
                assert opcode == 0x2 and payload == b"\x00\xff"

                second.close()
                clients.remove(second)
                deadline = time.monotonic() + 2
                while True:
                    try:
                        http_request(
                            endpoint,
                            port,
                            "execute-api",
                            "/@connections/" + urllib.parse.quote(ready_second["connectionId"], safe=""),
                            b"gone",
                            expected_status=410,
                        )
                        break
                    except AssertionError as error:
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(0.05)

                error_body = http_request(
                    endpoint,
                    port,
                    "iotdata",
                    f"/topics/{topic}?qos=2",
                    b"invalid",
                    expected_status=400,
                )
                assert b"InvalidRequestException" in error_body
                print("PASS: IoT topic subscriptions, wildcard delivery, binary payloads, and API Gateway callbacks")
            finally:
                for client in clients:
                    client.close()
                process.terminate()
                process.wait(timeout=10)


if __name__ == "__main__":
    main()
