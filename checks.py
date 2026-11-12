"""Baseline business regressions and supplier protocol checks; no external service."""
import asyncio
import base64
import copy
import json
import sqlite3
import sys
from archive import Archive
from portal import Portal, CHUNK, canonical
from vendor.export_v1 import Client as V1
from vendor.export_v2 import Client as V2, RemoteError

def source():
    with open("samples/source.json", encoding="utf-8") as handle:
        return json.load(handle)

def old_database():
    db = sqlite3.connect(":memory:")
    with open("samples/legacy.sql", encoding="utf-8") as handle:
        db.executescript(handle.read())
    return db

def expect_error(call, text):
    try:
        call()
    except Exception as error:
        assert str(error) == text, (str(error), text)
    else:
        raise AssertionError("MISSING_ERROR:" + text)

def normal(show=False):
    db = old_database()
    archive = Archive(db)
    remote = Portal(source())
    client = V1(transport=remote.dispatch)
    before = archive.read()
    assert before["packets"][0]["revision"] == 1
    assert [x["path"] for x in before["packets"][0]["files"]] == [
        "主表.pdf", "附件/A.txt", "附件/empty.txt"]
    assert archive.enqueue("old-1", "R-17", 1) == {
        "job": "old-1", "packet": "R-17", "revision": 1, "outcome": "published"}
    expect_error(lambda: archive.enqueue("old-1", "R-17", 2), "JOB_CONFLICT")
    dump = list(db.iterdump())
    expect_error(lambda: archive.run("pending-2", client, "before_receipt"), "INJECTED")
    assert list(db.iterdump()) == dump
    result = archive.run("pending-2", client)
    assert result["outcome"] == "published"
    assert archive.run("pending-2", client) == result
    archive.enqueue("late-1", "R-17", 1)
    assert archive.run("late-1", client)["outcome"] == "superseded"
    archive.enqueue("copy", "R-18", 1)
    archive.run("copy", client)
    after = archive.read()
    r17, r18 = after["packets"]
    assert r17["revision"] == 2
    assert [x["path"] for x in r17["files"]] == ["主表.pdf", "附件/a.txt"]
    assert r17["files"][1]["body"] == r18["files"][0]["body"] == "YWJjCg=="
    assert r17["files"][1]["media"] != r18["files"][0]["media"]
    if show:
        print(canonical({"before": before, "receipt": result, "after": after}))
    db.close()
    remote.db.close()
    print("NORMAL_REGRESSION_OK")

async def protocol(show=False):
    fixture = copy.deepcopy(source())
    raw = bytes(range(256)) * 300
    fixture["packets"].append({
        "packet": "large", "revision": 1,
        "files": [{"path": "large.bin", "media": "application/octet-stream",
                   "body": base64.b64encode(raw).decode()}]})
    remote = Portal(fixture, v1=False)
    client = V2(transport=remote.dispatch)
    opened = await client.open("archive-x:job-x", "large", 1)
    session = opened["session"]
    assert (await client.open("archive-x:job-x", "large", 1)) == opened
    first = await client.pull(session, "page-0", 0)
    assert len(base64.b64decode(first["body"])) == CHUNK
    assert first == await client.pull(session, "page-0", 0)
    try:
        await client.pull(session, "page-1", 1)
    except RemoteError as error:
        assert str(error) == "UNACKED"
    else:
        raise AssertionError("UNACKED_NOT_ENFORCED")
    status = await client.status(session)
    assert status["cursor"] == 1 and status["pending"] == "page-0"
    await client.ack(session, "page-0")
    try:
        await client.pull(session, "page-0", 0)
    except RemoteError as error:
        assert str(error) == "ACKED"
    else:
        raise AssertionError("ACKED_BYTES_STILL_AVAILABLE")
    assert remote.db.execute(
        "SELECT response FROM operations WHERE session=? AND operation='page-0'",
        (session,)).fetchone()[0] is None
    # Reconstruct the remote database, not the client: session identity survives.
    snapshot = "\n".join(remote.db.iterdump())
    remote.db.close()
    restored = sqlite3.connect(":memory:", check_same_thread=False)
    restored.executescript(snapshot)
    remote = Portal(fixture, restored, v1=False)
    client = V2(transport=remote.dispatch)
    assert (await client.open("archive-x:job-x", "large", 1)) == opened
    second = await client.pull(session, "page-1", 1)
    await client.ack(session, "page-0")  # late duplicate ack cannot clear page-1
    assert (await client.status(session))["pending"] == "page-1"
    assert base64.b64decode(first["body"]) + base64.b64decode(second["body"]) == raw
    await client.ack(session, "page-1")
    assert (await client.status(session))["done"] is True
    assert remote.dispatch("v1/export", {"packet": "large", "revision": 1}) == {
        "error": "V1_RETIRED"}
    if show:
        print(canonical({"session": session, "first_bytes": CHUNK,
                         "second_bytes": len(base64.b64decode(second["body"])),
                         "after_ack": await client.status(session),
                         "replay_after_ack": "ACKED"}))
    remote.db.close()
    print("SDK2_PROTOCOL_OK")

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if mode not in ("normal", "sdk", "verify", "demo"):
        raise SystemExit("unknown mode")
    if mode in ("normal", "verify", "demo"):
        normal(mode == "demo")
    if mode in ("sdk", "verify", "demo"):
        asyncio.run(protocol(mode == "demo"))
