"""Baseline business regressions, supplier protocol checks, and schema-2
migration / recovery / contention / CLI verification; no external service."""
import asyncio
import base64
import copy
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from archive import Archive
from portal import Portal, CHUNK, canonical
from vendor.export_v1 import Client as V1
from vendor.export_v2 import Client as V2, RemoteError

ROOT = os.path.dirname(os.path.abspath(__file__))
SOURCE = os.path.join(ROOT, "samples", "source.json")
LEGACY = os.path.join(ROOT, "samples", "legacy.sql")

def source():
    with open(SOURCE, encoding="utf-8") as handle:
        return json.load(handle)

def old_database():
    db = sqlite3.connect(":memory:")
    with open(LEGACY, encoding="utf-8") as handle:
        db.executescript(handle.read())
    return db

def expect_error(call, text):
    try:
        call()
    except Exception as error:
        assert str(error) == text, (str(error), text)
    else:
        raise AssertionError("MISSING_ERROR:" + text)

async def expect_error_async(call, text):
    try:
        await call()
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

# --------------------------------------------------------------------------
# schema-2 migration, recovery, contention and CLI checks
# --------------------------------------------------------------------------

def legacy_copy(path):
    db = sqlite3.connect(path)
    with open(LEGACY, encoding="utf-8") as handle:
        db.executescript(handle.read())
    return db

def migrated_archive(path):
    archive = Archive(legacy_copy(path))
    archive.migrate()
    return archive

class FlakyPortal:
    """Drops or corrupts supplier responses around the real dispatch."""
    def __init__(self, dispatch):
        self.dispatch = dispatch
        self.after = {}
        self.before = {}
        self.corrupt = None
    def drop_after(self, action, count=1):
        self.after[action] = count
    def drop_before(self, action, count=1):
        self.before[action] = count
    def __call__(self, action, request):
        if self.before.get(action):
            self.before[action] -= 1
            raise ConnectionError("lost before dispatch: " + action)
        result = self.dispatch(action, request)
        if action == "v2/pull" and self.corrupt:
            result = json.loads(canonical(result))
            raw = bytearray(base64.b64decode(result["body"]))
            if self.corrupt == "flip" and raw:
                raw[0] ^= 1
            elif self.corrupt == "zero":
                raw = bytearray()
            elif self.corrupt == "truncate" and raw:
                raw = raw[: len(raw) - 1]
            elif self.corrupt == "offset":
                result["offset"] = result["offset"] + 1
            result["body"] = base64.b64encode(bytes(raw)).decode()
        if self.after.get(action):
            self.after[action] -= 1
            raise ConnectionError("lost after dispatch: " + action)
        return result

async def drive(archive, job, client, worker="W", now=1000, ttl=30, limit=64):
    for _ in range(limit):
        result = await archive.advance(job, client, worker, now, ttl)
        if result["state"] == "done":
            return result["receipt"]
        assert result["state"] == "pending", result
    raise AssertionError("NO_PROGRESS")

def pull_cursors(remote):
    return [json.loads(row[0])["cursor"] for row in remote.db.execute(
        "SELECT request FROM traffic WHERE action='v2/pull' ORDER BY seq")]

def migration(show=False):
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, "archive.sqlite")
    old_archive = Archive(legacy_copy(path))  # process built before migration
    before = old_archive.read()
    archive = Archive(sqlite3.connect(path))
    assert archive.migrate() == {"version": 2}
    assert archive.migrate() == {"version": 2}  # idempotent
    fresh = sqlite3.connect(path)
    assert fresh.execute("PRAGMA user_version").fetchone()[0] == 2
    after = archive.read()
    assert after == before  # identical files, media, ordering across migration
    assert archive.db.execute("SELECT archive_id FROM meta").fetchone()[0] == \
        "site-archive-2026"  # archive identity preserved
    assert archive.db.execute(
        "SELECT result FROM receipts WHERE job='old-1'").fetchone()[0] == \
        '{"job":"old-1","outcome":"published","packet":"R-17","revision":1}'
    assert archive.db.execute("SELECT binding FROM queue WHERE job='pending-2'") \
        .fetchone()[0] == \
        "0f61f0dd5d495276ffd4b0e5fd5b5d90952a307239620e74b0e4ad1beaf8047e"
    # payload deduplicated into objects; legacy payload tables are gone
    assert archive.db.execute("SELECT COUNT(*) FROM objects").fetchone()[0] == 3
    assert archive.db.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND "
        "name IN ('files','packets','jobs')").fetchone()[0] == 0
    # enqueue/read compatibility on the migrated library
    assert archive.enqueue("old-1", "R-17", 1) == {
        "job": "old-1", "packet": "R-17", "revision": 1, "outcome": "published"}
    expect_error(lambda: archive.enqueue("old-1", "R-17", 2), "JOB_CONFLICT")
    assert archive.enqueue("new-1", "R-18", 1) == {"job": "new-1",
                                                   "outcome": "pending"}
    # old process may still read existing receipts ...
    assert old_archive.enqueue("old-1", "R-17", 1)["outcome"] == "published"
    # ... but any late old-style write fails at the database and rolls back
    remote = Portal(source())
    legacy_client = V1(transport=remote.dispatch)
    dump = list(fresh.iterdump())
    expect_error(lambda: old_archive.run("pending-2", legacy_client),
                 "no such table: jobs")
    expect_error(lambda: old_archive.enqueue("brand-new", "R-18", 1),
                 "no such table: jobs")
    assert list(fresh.iterdump()) == dump
    # the SDK1 whole-package run is a schema1-only path
    expect_error(lambda: archive.run("pending-2", legacy_client),
                 "SCHEMA_UNSUPPORTED")
    if show:
        print(canonical({"migrated": after, "objects": 3,
                         "old_process_write": "rejected"}))
    fresh.close()
    archive.db.close()
    old_archive.db.close()
    remote.db.close()
    print("MIGRATION_OK")

async def recovery(show=False):
    tmp = tempfile.mkdtemp()
    report = {}

    # A: ack committed remotely but its response was lost; lease busy, then
    # expired takeover finishes the job without re-pulling acked segments.
    archive = migrated_archive(os.path.join(tmp, "a.sqlite"))
    remote = Portal(source(), v1=False)
    flaky = FlakyPortal(remote.dispatch)
    client = V2(transport=flaky)
    # input validation and unknown-job handling before any state change
    await expect_error_async(
        lambda: archive.advance("no-such", client, "W", 0, 1), "NO_JOB")
    await expect_error_async(
        lambda: archive.advance("pending-2", client, "", 0, 1), "INPUT")
    await expect_error_async(
        lambda: archive.advance("pending-2", client, "W", -1, 1), "INPUT")
    await expect_error_async(
        lambda: archive.advance("pending-2", client, "W", True, 1), "INPUT")
    await expect_error_async(
        lambda: archive.advance("pending-2", client, "W", 0, 0), "INPUT")
    await expect_error_async(
        lambda: archive.advance("pending-2", client, "W", 0, True), "INPUT")
    flaky.drop_after("v2/ack")
    try:
        await archive.advance("pending-2", client, "A", 100, 30)
        raise AssertionError("EXPECTED_DROPPED_ACK")
    except ConnectionError:
        pass
    busy = await archive.advance("pending-2", client, "B", 101, 30)
    assert busy == {"job": "pending-2", "state": "busy", "receipt": None}
    done = await archive.advance("pending-2", client, "B", 130, 30)
    assert done["state"] == "done"
    assert done["receipt"] == {"job": "pending-2", "packet": "R-17",
                               "revision": 2, "outcome": "published"}
    assert pull_cursors(remote) == [0, 1]  # acked segment never re-fetched
    assert remote.db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 1
    view = archive.read()
    assert view["packets"][0]["revision"] == 2
    assert [f["path"] for f in view["packets"][0]["files"]] == [
        "主表.pdf", "附件/a.txt"]
    assert archive.enqueue("old-1", "R-17", 1) == {
        "job": "old-1", "packet": "R-17", "revision": 1, "outcome": "published"}
    archive.enqueue("late-1", "R-17", 1)
    receipt = await drive(archive, "late-1", client)
    assert receipt["outcome"] == "superseded"
    assert archive.read()["packets"][0]["revision"] == 2  # head not regressed
    report["takeover"] = done["receipt"]
    report["late"] = receipt
    archive.db.close()
    remote.db.close()

    # B: open and pull each committed remotely but their responses were lost;
    # the consumer/session survive and the unacked operation is re-fetched
    # with identical bytes instead of starting over.
    archive = migrated_archive(os.path.join(tmp, "b.sqlite"))
    archive.enqueue("j1", "R-18", 1)
    remote = Portal(source(), v1=False)
    flaky = FlakyPortal(remote.dispatch)
    flaky.drop_after("v2/open")
    flaky.drop_after("v2/pull")
    client = V2(transport=flaky)
    for moment in (100, 200):  # second call takes over the expired lease
        try:
            await archive.advance("j1", client, "A", moment, 30)
            raise AssertionError("EXPECTED_DROPPED_RESPONSE")
        except ConnectionError:
            pass
    receipt = await drive(archive, "j1", client)
    assert receipt == {"job": "j1", "packet": "R-18", "revision": 1,
                       "outcome": "published"}
    assert pull_cursors(remote) == [0, 0]  # same unacked operation re-pulled
    assert remote.db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 1
    r18 = {p["packet"]: p for p in archive.read()["packets"]}["R-18"]
    assert r18["files"][0]["body"] == "YWJjCg=="
    report["repull"] = receipt
    archive.db.close()
    remote.db.close()

    # C: crash after the segment persist, before the ack was ever dispatched.
    archive = migrated_archive(os.path.join(tmp, "c.sqlite"))
    archive.enqueue("j1", "R-17", 2)
    remote = Portal(source(), v1=False)
    flaky = FlakyPortal(remote.dispatch)
    flaky.drop_before("v2/ack")
    client = V2(transport=flaky)
    try:
        await archive.advance("j1", client, "A", 100, 30)
        raise AssertionError("EXPECTED_FAILED_ACK")
    except ConnectionError:
        pass
    receipt = await drive(archive, "j1", client)
    assert receipt["outcome"] == "published"
    assert archive.read()["packets"][0]["revision"] == 2
    report["unacked"] = receipt
    archive.db.close()
    remote.db.close()

    # D: corrupted or truncated payloads never masquerade as success; the
    # failure reason is persisted and the current packet stays untouched.
    archive = migrated_archive(os.path.join(tmp, "d.sqlite"))
    for job in ("bad-1", "bad-2", "bad-3", "bad-4"):
        archive.enqueue(job, "R-17", 2)
    remote = Portal(source(), v1=False)
    flaky = FlakyPortal(remote.dispatch)
    flaky.corrupt = "flip"
    client = V2(transport=flaky)
    await expect_error_async(
        lambda: drive(archive, "bad-1", client), "CONTENT_MISMATCH")
    assert archive.db.execute(
        "SELECT failed FROM transfers WHERE job='bad-1'").fetchone()[0]
    assert archive.read()["packets"][0]["revision"] == 1  # packet unchanged
    assert archive.db.execute(
        "SELECT COUNT(*) FROM receipts WHERE job='bad-1'").fetchone()[0] == 0
    await expect_error_async(
        lambda: archive.advance("bad-1", client, "W", 500, 30), "CONTENT_MISMATCH")
    flaky.corrupt = "zero"  # zero-byte segment where bytes are owed
    await expect_error_async(
        lambda: archive.advance("bad-2", client, "W", 600, 30), "CONTENT_MISMATCH")
    flaky.corrupt = "truncate"  # truncated segment body
    await expect_error_async(
        lambda: archive.advance("bad-3", client, "W", 700, 30), "CONTENT_MISMATCH")
    flaky.corrupt = "offset"  # segment metadata disagrees with the manifest
    await expect_error_async(
        lambda: archive.advance("bad-4", client, "W", 800, 30), "CONTENT_MISMATCH")
    report["mismatch"] = "persisted"
    archive.db.close()
    remote.db.close()

    # F: a worker whose lease expires mid-call is taken over; its late return
    # cannot rewrite progress or publish.
    import threading
    archive = migrated_archive(os.path.join(tmp, "f.sqlite"))
    archive.enqueue("j1", "R-17", 2)
    remote = Portal(source(), v1=False)
    gate = threading.Event()
    real_dispatch = remote.dispatch
    def gated(action, request):
        result = real_dispatch(action, request)
        if action == "v2/pull":
            gate.wait(10)  # hold A's first pull response inside the call
        return result
    stale_client = V2(transport=gated)
    fresh_client = V2(transport=real_dispatch)
    stale_call = asyncio.ensure_future(
        archive.advance("j1", stale_client, "A", 100, 30))
    for _ in range(200):  # wait until A's pull is genuinely in flight
        if remote.db.execute(
                "SELECT COUNT(*) FROM traffic WHERE action='v2/pull'") \
                .fetchone()[0]:
            break
        await asyncio.sleep(0.02)
    else:
        raise AssertionError("STALE_CALL_NOT_IN_FLIGHT")
    receipt = await drive(archive, "j1", fresh_client, worker="B", now=200)
    assert receipt["outcome"] == "published"  # B took over and finished
    gate.set()
    stale_result = await stale_call
    assert stale_result == {"job": "j1", "state": "busy", "receipt": None}
    assert archive.db.execute(
        "SELECT COUNT(*) FROM receipts WHERE job='j1'").fetchone()[0] == 1
    assert remote.db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 1
    assert archive.read()["packets"][0]["revision"] == 2
    report["stale"] = stale_result["state"]
    archive.db.close()
    remote.db.close()

    # E: multi-segment files, CHUNK-exact files, empty files and empty packets
    # survive a chunked transfer byte-exactly.
    fixture = copy.deepcopy(source())
    big_raw = bytes(range(256)) * 300    # 76800 bytes -> two segments
    exact_raw = bytes(range(256)) * 256  # exactly one CHUNK
    fixture["packets"].append({
        "packet": "BIG", "revision": 1,
        "files": [{"path": "big.bin", "media": "application/octet-stream",
                   "body": base64.b64encode(big_raw).decode()},
                  {"path": "exact.bin", "media": "application/octet-stream",
                   "body": base64.b64encode(exact_raw).decode()},
                  {"path": "empty.txt", "media": "text/plain", "body": ""}]})
    fixture["packets"].append({"packet": "EMPTY", "revision": 1, "files": []})
    archive = migrated_archive(os.path.join(tmp, "e.sqlite"))
    archive.enqueue("big-1", "BIG", 1)
    archive.enqueue("empty-1", "EMPTY", 1)
    remote = Portal(fixture, v1=False)
    client = V2(transport=remote.dispatch)
    assert (await drive(archive, "big-1", client))["outcome"] == "published"
    assert (await drive(archive, "empty-1", client))["outcome"] == "published"
    view = {p["packet"]: p for p in archive.read()["packets"]}
    big = {f["path"]: f for f in view["BIG"]["files"]}
    assert base64.b64decode(big["big.bin"]["body"]) == big_raw
    assert base64.b64decode(big["exact.bin"]["body"]) == exact_raw
    assert big["empty.txt"]["body"] == ""
    assert big["big.bin"]["sha256"] == hashlib.sha256(big_raw).hexdigest()
    assert view["EMPTY"]["files"] == []
    report["big"] = {"segments": 4, "bytes": len(big_raw)}
    archive.db.close()
    remote.db.close()

    if show:
        print(canonical(report))
    print("RECOVERY_OK")

async def contention(show=False):
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, "archive.sqlite")
    archive = migrated_archive(path)
    other = Archive(sqlite3.connect(path))
    archive.enqueue("copy", "R-18", 1)
    remote = Portal(source(), v1=False)
    client = V2(transport=remote.dispatch)

    async def loop(handle, job):
        while True:
            result = await handle.advance(job, client, "W", 1000, 30)
            if result["state"] == "done":
                return result["receipt"]
            await asyncio.sleep(0)

    # same job, same worker string, two archive connections: safe and identical
    first, second = await asyncio.gather(loop(archive, "pending-2"),
                                         loop(other, "pending-2"))
    assert first == second == {"job": "pending-2", "packet": "R-17",
                               "revision": 2, "outcome": "published"}
    assert archive.db.execute(
        "SELECT COUNT(*) FROM receipts WHERE job='pending-2'").fetchone()[0] == 1

    # different packets advance in parallel on one connection: network waits
    # never hold a local write transaction or a global lock
    archive.enqueue("late-1", "R-17", 1)
    done_copy, done_late = await asyncio.gather(loop(archive, "copy"),
                                                loop(archive, "late-1"))
    assert done_copy["outcome"] == "published"
    assert done_late["outcome"] == "superseded"
    view = {p["packet"]: p for p in archive.read()["packets"]}
    assert view["R-17"]["revision"] == 2
    assert [f["path"] for f in view["R-17"]["files"]] == ["主表.pdf", "附件/a.txt"]
    assert [f["path"] for f in view["R-18"]["files"]] == ["copy.bin"]
    # shared content kept once, referenced with independent media types
    shared = hashlib.sha256(b"abc\n").hexdigest()
    assert archive.db.execute("SELECT COUNT(*) FROM objects WHERE digest=?",
                              (shared,)).fetchone()[0] == 1
    r17_file = view["R-17"]["files"][1]
    r18_file = view["R-18"]["files"][0]
    assert r17_file["body"] == r18_file["body"] == "YWJjCg=="
    assert r17_file["media"] == "text/plain"
    assert r18_file["media"] == "application/octet-stream"
    if show:
        print(canonical({"same_job": first, "parallel": [done_copy, done_late],
                         "objects": archive.db.execute(
                             "SELECT COUNT(*) FROM objects").fetchone()[0]}))
    archive.db.close()
    other.db.close()
    remote.db.close()
    print("CONTENTION_OK")

def cli(show=False):
    tmp = tempfile.mkdtemp()

    def run_cli(*args, expect=0):
        proc = subprocess.run([sys.executable, os.path.join(ROOT, "run.py"), *args],
                              capture_output=True, text=True, cwd=ROOT)
        assert proc.returncode == expect, (proc.args, proc.stdout, proc.stderr)
        return json.loads(proc.stdout)

    def steps(db, job, remote, worker="W", now=100, ttl=30):
        for _ in range(16):
            out = run_cli("--db", db, "step", job, SOURCE, "--remote", remote,
                          "--worker", worker, "--now", str(now), "--ttl", str(ttl))
            if out["state"] == "done":
                return out["receipt"]
            assert out["state"] == "pending" and out["receipt"] is None, out
        raise AssertionError("STEP_LOOP_STUCK")

    report = {}
    # legacy CLI on schema1 stays intact
    db1 = os.path.join(tmp, "one.sqlite")
    legacy_copy(db1).close()
    before = run_cli("--db", db1, "read")
    assert before["packets"][0]["revision"] == 1
    assert run_cli("--db", db1, "enqueue", "extra-1", "R-18", "1") == {
        "job": "extra-1", "outcome": "pending"}
    assert run_cli("--db", db1, "migrate") == {"version": 2}
    assert run_cli("--db", db1, "migrate") == {"version": 2}
    remote1 = os.path.join(tmp, "one.remote.sqlite")
    receipt = steps(db1, "pending-2", remote1)
    assert receipt == {"job": "pending-2", "packet": "R-17", "revision": 2,
                       "outcome": "published"}
    assert steps(db1, "extra-1", remote1)["outcome"] == "published"
    after = run_cli("--db", db1, "read")
    view = {p["packet"]: p for p in after["packets"]}
    assert [f["path"] for f in view["R-17"]["files"]] == ["主表.pdf", "附件/a.txt"]
    assert view["R-17"]["revision"] == 2
    assert view["R-18"]["files"][0]["body"] == "YWJjCg=="
    assert run_cli("--db", db1, "enqueue", "old-1", "R-17", "1") == {
        "job": "old-1", "packet": "R-17", "revision": 1, "outcome": "published"}
    report["step_receipt"] = receipt

    # work on an unmigrated database keeps the SDK1 path
    db2 = os.path.join(tmp, "two.sqlite")
    legacy_copy(db2).close()
    assert run_cli("--db", db2, "work", "pending-2", SOURCE) == receipt

    # work on a migrated database uses SDK2 against the persistent remote
    # file and resumes a half-consumed cursor instead of restarting it
    db3 = os.path.join(tmp, "three.sqlite")
    legacy_copy(db3).close()
    assert run_cli("--db", db3, "migrate") == {"version": 2}
    remote3 = db3 + ".remote.sqlite"
    first = run_cli("--db", db3, "step", "pending-2", SOURCE, "--remote",
                    remote3, "--worker", "W", "--now", "100", "--ttl", "30")
    assert first["state"] == "pending"  # exactly one segment consumed
    assert run_cli("--db", db3, "work", "pending-2", SOURCE) == receipt
    assert os.path.exists(remote3)
    remote_db = sqlite3.connect(remote3)
    assert [json.loads(row[0])["cursor"] for row in remote_db.execute(
        "SELECT request FROM traffic WHERE action='v2/pull' ORDER BY seq")] == [0, 1]
    remote_db.close()
    assert run_cli("--db", db3, "work", "pending-2", SOURCE) == receipt
    report["work_receipt"] = receipt

    # step auto-migrates an old library before consuming
    db4 = os.path.join(tmp, "four.sqlite")
    legacy_copy(db4).close()
    assert run_cli("--db", db4, "enqueue", "j1", "R-18", "1") == {
        "job": "j1", "outcome": "pending"}
    done = run_cli("--db", db4, "step", "j1", SOURCE, "--remote",
                   os.path.join(tmp, "four.remote.sqlite"), "--worker", "W",
                   "--now", "100", "--ttl", "30")
    assert done["state"] == "done"
    assert done["receipt"]["outcome"] == "published"
    report["auto_migrate"] = done["receipt"]
    if show:
        print(canonical(report))
    print("CLI_OK")

def memory(show=False):
    """Payload residency during a chunked transfer stays proportional to
    CHUNK and does not grow with the file: an 8 MiB single-file package is
    archived while the archive process peaks far below the file size.  The
    supplier runs in its own process over real HTTP so only archive-side
    memory is measured."""
    import tracemalloc
    tmp = tempfile.mkdtemp()
    server_code = "\n".join([
        "import base64, os, sys, time",
        "sys.path.insert(0, %r)" % ROOT,
        "from portal import Portal, serving",
        "raw = os.urandom(%d)" % (8 * 1024 * 1024),
        "fixture = {'packets': [{'packet': 'HUGE', 'revision': 1, 'files': ["
        "{'path': 'huge.bin', 'media': 'application/octet-stream',"
        " 'body': base64.b64encode(raw).decode()}]}]}",
        "remote = Portal(fixture, v1=False)",
        "with serving(remote, port=0) as origin:",
        "    print(origin, flush=True)",
        "    time.sleep(300)",
    ])
    server = subprocess.Popen([sys.executable, "-c", server_code],
                              stdout=subprocess.PIPE, text=True, cwd=ROOT)
    try:
        origin = server.stdout.readline().strip()
        path = os.path.join(tmp, "mem.sqlite")
        legacy_copy(path).close()
        archive = Archive(sqlite3.connect(path))
        archive.migrate()
        archive.enqueue("huge-1", "HUGE", 1)
        client = V2(origin=origin)
        tracemalloc.start()
        receipt = asyncio.run(drive(archive, "huge-1", client, limit=200))
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        assert receipt["outcome"] == "published"
        assert archive.db.execute(
            "SELECT length(body) FROM objects WHERE digest = "
            "(SELECT digest FROM refs WHERE packet='HUGE')").fetchone()[0] == \
            8 * 1024 * 1024
        assert peak < 16 * CHUNK, peak  # ~10x CHUNK measured; never file-sized
        if show:
            print(canonical({"segments": 128, "file_bytes": 8 * 1024 * 1024,
                             "peak_python_bytes": peak,
                             "chunk_ratio": round(peak / CHUNK, 2)}))
        archive.db.close()
    finally:
        server.kill()
    print("MEMORY_OK")

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if mode not in ("normal", "sdk", "verify", "demo"):
        raise SystemExit("unknown mode")
    show = mode == "demo"
    if mode in ("normal", "verify", "demo"):
        normal(show)
    if mode in ("sdk", "verify", "demo"):
        asyncio.run(protocol(show))
    if mode in ("verify", "demo"):
        migration(show)
        asyncio.run(recovery(show))
        asyncio.run(contention(show))
        cli(show)
        memory(show)
