"""Supplier export SDK 1.4: synchronous, whole-package response."""
import json
import urllib.request

class Client:
    def __init__(self, origin="", transport=None):
        self.origin = origin.rstrip("/")
        self.transport = transport

    def export(self, packet, revision):
        payload = {"packet": packet, "revision": revision}
        if self.transport:
            result = self.transport("v1/export", payload)
        else:
            req = urllib.request.Request(
                self.origin + "/v1/export", json.dumps(payload).encode(),
                {"Content-Type": "application/json"})
            with urllib.request.urlopen(req) as response:
                result = json.load(response)
        if "error" in result:
            raise RuntimeError(result["error"])
        return result
