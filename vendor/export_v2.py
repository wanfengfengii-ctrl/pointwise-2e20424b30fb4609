"""Supplier export SDK 2.0. No implicit retries, buffering or local checkpoint."""
import asyncio
import json
import urllib.request

class RemoteError(RuntimeError):
    pass

class Client:
    def __init__(self, origin="", transport=None):
        self.origin = origin.rstrip("/")
        self.transport = transport

    def _request(self, action, payload):
        if self.transport:
            result = self.transport("v2/" + action, payload)
            if "error" in result:
                raise RemoteError(result["error"])
            return result
        request = urllib.request.Request(
            self.origin + "/v2/" + action,
            json.dumps(payload).encode(),
            {"Content-Type": "application/json"})
        with urllib.request.urlopen(request) as response:
            result = json.load(response)
        if "error" in result:
            raise RemoteError(result["error"])
        return result

    async def open(self, consumer, packet, revision):
        return await asyncio.to_thread(self._request, "open", {
            "consumer": consumer, "packet": packet, "revision": revision})

    async def status(self, session):
        return await asyncio.to_thread(self._request, "status", {"session": session})

    async def pull(self, session, operation, cursor):
        return await asyncio.to_thread(self._request, "pull", {
            "session": session, "operation": operation, "cursor": cursor})

    async def ack(self, session, operation):
        return await asyncio.to_thread(self._request, "ack", {
            "session": session, "operation": operation})
