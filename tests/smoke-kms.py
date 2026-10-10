#!/usr/bin/env python3
"""KMS HTTP contract checks using only the Python standard library."""
import base64
import json
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = subprocess.Popen(
        [str(ROOT / "target/debug/roto-server"), "--ephemeral", "--setup",
         str(ROOT / "examples/demo/setup.lua"), "--port", str(port)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}"

    def call(op, data, expected=200):
        request = urllib.request.Request(url, data=json.dumps(data).encode(), headers={
            "Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": f"TrentService.{op}",
            "Authorization": "AWS4-HMAC-SHA256 Credential=demo/20261010/us-east-1/kms/aws4_request, SignedHeaders=host, Signature=fake",
        })
        try:
            response = urllib.request.urlopen(request, timeout=5)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            body = json.load(response)
            assert response.status == expected, (op, response.status, body)
            return body

    try:
        for _ in range(200):
            if server.poll() is not None:
                raise AssertionError("KMS smoke server exited")
            try:
                with urllib.request.urlopen(url + "/roto-api/health", timeout=1):
                    break
            except (OSError, urllib.error.URLError):
                time.sleep(0.05)
        else:
            raise AssertionError("KMS smoke server did not become healthy")
        key = call("CreateKey", {})["KeyMetadata"]
        call("CreateAlias", {"AliasName": "alias/smoke", "TargetKeyId": key["KeyId"]})
        plain = base64.b64encode(b"\x00\xffbinary\x80").decode()
        enc = call("Encrypt", {"KeyId": "alias/smoke", "Plaintext": plain,
                               "EncryptionContext": {"purpose": "smoke"}})
        env = json.loads(base64.b64decode(enc["CiphertextBlob"]))
        assert env["roto_kms"] == 1 and env["plaintext"] == plain
        decrypt = {"CiphertextBlob": enc["CiphertextBlob"], "EncryptionContext": {"purpose": "smoke"}}
        assert call("Decrypt", decrypt)["Plaintext"] == plain
        assert call("Decrypt", {"CiphertextBlob": enc["CiphertextBlob"]}, 400)["__type"].endswith("InvalidCiphertextException")
        call("DisableKey", {"KeyId": key["Arn"]})
        assert call("Decrypt", decrypt, 400)["__type"].endswith("DisabledException")
        call("EnableKey", {"KeyId": key["KeyId"]})
        data = call("GenerateDataKey", {"KeyId": key["Arn"], "KeySpec": "AES_256"})
        assert len(base64.b64decode(data["Plaintext"])) == 32
        assert call("Decrypt", {"CiphertextBlob": data["CiphertextBlob"]})["Plaintext"] == data["Plaintext"]
        assert call("Sign", {"KeyId": key["Arn"], "Message": plain, "SigningAlgorithm": "RSASSA_PSS_SHA_256"}, 501)["__type"].endswith("NotImplemented")
        print("KMS HTTP round trips, envelope, context, state and data keys passed")
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()


if __name__ == "__main__":
    main()
