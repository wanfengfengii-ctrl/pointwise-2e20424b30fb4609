"""Legacy archive (schema1/SDK1) plus schema-2 migration and the SDK-2 cursor consumer.

schema2 stores each distinct payload once in objects(digest, body); every
(packet, path) reference keeps its own media and points at a digest.  The
schema1 payload tables (files/packets/jobs) are dropped by migrate(), so a
legacy process constructed before the migration fails and rolls back any
late write, while reading existing receipts keeps working.  Transfer state
(transfers/segments/done_files) makes every persistence boundary
recoverable: a segment is durable before it is acked, acks are idempotent,
and a lease token (never the worker name) guards every local write.
"""
import base64
import hashlib
import json
import sqlite3
import uuid
from portal import canonical, CHUNK
from vendor.export_v2 import RemoteError

DDL = """
CREATE TABLE meta(archive_id TEXT NOT NULL);
CREATE TABLE packets(packet TEXT PRIMARY KEY, revision INTEGER NOT NULL, digest TEXT NOT NULL);
CREATE TABLE files(packet TEXT, path TEXT, media TEXT, body BLOB NOT NULL,
                   PRIMARY KEY(packet,path));
CREATE TABLE jobs(job TEXT PRIMARY KEY, packet TEXT NOT NULL, revision INTEGER NOT NULL,
                  binding TEXT NOT NULL);
CREATE TABLE receipts(job TEXT PRIMARY KEY, binding TEXT NOT NULL, result TEXT NOT NULL);
PRAGMA user_version=1;
"""

DDL2 = [
    "CREATE TABLE objects(digest TEXT PRIMARY KEY, body BLOB NOT NULL)",
    "CREATE TABLE heads(packet TEXT PRIMARY KEY, revision INTEGER NOT NULL, "
    "digest TEXT NOT NULL)",
    "CREATE TABLE refs(packet TEXT NOT NULL, path TEXT NOT NULL, media TEXT NOT NULL, "
    "digest TEXT NOT NULL, PRIMARY KEY(packet,path))",
    "CREATE TABLE queue(job TEXT PRIMARY KEY, packet TEXT NOT NULL, "
    "revision INTEGER NOT NULL, binding TEXT NOT NULL)",
    "CREATE TABLE transfers(job TEXT PRIMARY KEY, consumer TEXT NOT NULL, "
    "session TEXT, manifest TEXT, next_cursor INTEGER NOT NULL DEFAULT 0, "
    "pending_op TEXT, lease_token TEXT, lease_until INTEGER, failed TEXT)",
    "CREATE TABLE segments(job TEXT NOT NULL, seq INTEGER NOT NULL, body BLOB NOT NULL, "
    "PRIMARY KEY(job,seq))",
    "CREATE TABLE done_files(job TEXT NOT NULL, path TEXT NOT NULL, media TEXT NOT NULL, "
    "digest TEXT NOT NULL, PRIMARY KEY(job,path))",
]

def binding(packet, revision):
    return hashlib.sha256(canonical([packet, revision]).encode()).hexdigest()

def package_digest(files):
    manifest = []
    for item in sorted(files, key=lambda x: x["path"]):
        body = base64.b64decode(item["body"], validate=True)
        manifest.append({"path": item["path"], "media": item["media"],
                         "size": len(body), "sha256": hashlib.sha256(body).hexdigest()})
    return hashlib.sha256(canonical(manifest).encode()).hexdigest()

def manifest_digest(files):
    ordered = sorted(files, key=lambda x: x["path"])
    return hashlib.sha256(canonical(ordered).encode()).hexdigest()

def segment_map(files):
    """Flat cursor plan implied by a manifest: one entry per segment.

    Each entry is (path, offset, length, file_index, is_last_of_file).
    Empty files still produce exactly one (empty) segment."""
    plan = []
    for index, entry in enumerate(files):
        size = entry["size"]
        if size == 0:
            plan.append((entry["path"], 0, 0, index, True))
            continue
        offset = 0
        while offset < size:
            length = min(CHUNK, size - offset)
            plan.append((entry["path"], offset, length, index, offset + length == size))
            offset += length
    return plan

class LeaseLost(Exception):
    pass

class Archive:
    def __init__(self, db):
        self.db = db
        self.db.execute("PRAGMA busy_timeout=5000")
        self.version = self.db.execute("PRAGMA user_version").fetchone()[0]
        if self.version == 0:
            self.db.executescript(DDL)
            with self.db:
                self.db.execute("INSERT INTO meta VALUES(?)", (uuid.uuid4().hex,))
            self.version = 1
        elif self.version not in (1, 2):
            raise ValueError("SCHEMA_UNSUPPORTED")

    def _archive_id(self):
        return self.db.execute("SELECT archive_id FROM meta").fetchone()[0]

    # ------------------------------------------------------------------ enqueue
    def enqueue(self, job, packet, revision):
        if not all(isinstance(x, str) and x for x in (job, packet)):
            raise ValueError("INPUT")
        if type(revision) is not int or revision < 1:
            raise ValueError("INPUT")
        digest = binding(packet, revision)
        table = "jobs" if self.version == 1 else "queue"
        with self.db:
            old = self.db.execute("SELECT binding,result FROM receipts WHERE job=?", (job,)).fetchone()
            if old:
                if old[0] != digest:
                    raise ValueError("JOB_CONFLICT")
                return json.loads(old[1])
            old = self.db.execute("SELECT binding FROM %s WHERE job=?" % table, (job,)).fetchone()
            if old and old[0] != digest:
                raise ValueError("JOB_CONFLICT")
            self.db.execute("INSERT OR IGNORE INTO %s VALUES(?,?,?,?)" % table,
                            (job, packet, revision, digest))
        return {"job": job, "outcome": "pending"}

    # ------------------------------------------------------------- legacy run
    def run(self, job, client, fail_at=None):
        if self.version != 1:
            raise ValueError("SCHEMA_UNSUPPORTED")
        old = self.db.execute("SELECT result FROM receipts WHERE job=?", (job,)).fetchone()
        if old:
            return json.loads(old[0])
        request = self.db.execute(
            "SELECT packet,revision,binding FROM jobs WHERE job=?", (job,)).fetchone()
        if not request:
            raise ValueError("NO_JOB")
        packet, revision, digest = request
        exported = client.export(packet, revision)
        if exported["packet"] != packet or exported["revision"] != revision:
            raise ValueError("WRONG_EXPORT")
        files = exported["files"]
        if len({f["path"] for f in files}) != len(files):
            raise ValueError("DUPLICATE_PATH")
        content_digest = package_digest(files)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            old = self.db.execute("SELECT result FROM receipts WHERE job=?", (job,)).fetchone()
            if old:
                self.db.commit()
                return json.loads(old[0])
            head = self.db.execute(
                "SELECT revision,digest FROM packets WHERE packet=?", (packet,)).fetchone()
            if head and head[0] == revision and head[1] != content_digest:
                raise ValueError("REVISION_CONFLICT")
            outcome = "superseded" if head and head[0] > revision else "published"
            result = {"job": job, "packet": packet, "revision": revision, "outcome": outcome}
            if outcome == "published":
                self.db.execute("DELETE FROM files WHERE packet=?", (packet,))
                for f in files:
                    self.db.execute("INSERT INTO files VALUES(?,?,?,?)",
                                    (packet, f["path"], f["media"],
                                     base64.b64decode(f["body"], validate=True)))
                self.db.execute("INSERT OR REPLACE INTO packets VALUES(?,?,?)",
                                (packet, revision, content_digest))
            if fail_at == "before_receipt":
                raise RuntimeError("INJECTED")
            self.db.execute("INSERT INTO receipts VALUES(?,?,?)",
                            (job, digest, canonical(result)))
            self.db.execute("DELETE FROM jobs WHERE job=?", (job,))
            self.db.commit()
            return result
        except BaseException:
            self.db.rollback()
            raise

    # -------------------------------------------------------------------- read
    def read(self):
        if self.version == 2:
            return self._read_v2()
        self.db.execute("BEGIN")
        try:
            packets = []
            for packet, revision in self.db.execute(
                    "SELECT packet,revision FROM packets ORDER BY packet"):
                files = [{"path": path, "media": media,
                          "body": base64.b64encode(body).decode(),
                          "sha256": hashlib.sha256(body).hexdigest()}
                         for path, media, body in self.db.execute(
                             "SELECT path,media,body FROM files WHERE packet=? ORDER BY path",
                             (packet,))]
                packets.append({"packet": packet, "revision": revision, "files": files})
            self.db.commit()
            return {"packets": packets}
        except BaseException:
            self.db.rollback()
            raise

    def _read_v2(self):
        self.db.execute("BEGIN")
        try:
            packets = []
            for packet, revision in self.db.execute(
                    "SELECT packet,revision FROM heads ORDER BY packet"):
                files = []
                for path, media, digest in self.db.execute(
                        "SELECT path,media,digest FROM refs WHERE packet=? ORDER BY path",
                        (packet,)):
                    body = self.db.execute("SELECT body FROM objects WHERE digest=?",
                                           (digest,)).fetchone()[0]
                    files.append({"path": path, "media": media,
                                  "body": base64.b64encode(body).decode(),
                                  "sha256": hashlib.sha256(body).hexdigest()})
                packets.append({"packet": packet, "revision": revision, "files": files})
            self.db.commit()
            return {"packets": packets}
        except BaseException:
            self.db.rollback()
            raise

    # ----------------------------------------------------------------- migrate
    def migrate(self):
        """Atomically switch a schema1 database to schema2.  Idempotent."""
        version = self.db.execute("PRAGMA user_version").fetchone()[0]
        if version == 2:
            self.version = 2
            return {"version": 2}
        if version != 1:
            raise ValueError("SCHEMA_UNSUPPORTED")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            if self.db.execute("PRAGMA user_version").fetchone()[0] == 2:
                self.db.commit()
                self.version = 2
                return {"version": 2}
            for statement in DDL2:
                self.db.execute(statement)
            self.db.execute("INSERT INTO heads SELECT * FROM packets")
            self.db.execute("INSERT INTO queue SELECT * FROM jobs")
            rows = self.db.execute(
                "SELECT rowid,packet,path,media,length(body) FROM files").fetchall()
            for rowid, packet, path, media, size in rows:
                digest = self._blob_digest("files", rowid)
                if not self.db.execute("SELECT 1 FROM objects WHERE digest=?",
                                       (digest,)).fetchone():
                    if size:
                        self.db.execute("INSERT INTO objects VALUES(?, zeroblob(?))",
                                        (digest, size))
                        object_row = self.db.execute(
                            "SELECT rowid FROM objects WHERE digest=?",
                            (digest,)).fetchone()[0]
                        self._blob_copy("files", rowid, "objects", object_row)
                    else:
                        self.db.execute("INSERT INTO objects VALUES(?,?)", (digest, b""))
                self.db.execute("INSERT INTO refs VALUES(?,?,?,?)",
                                (packet, path, media, digest))
            # Dropping the legacy payload tables makes any late schema1 write
            # fail and roll back; receipts stay readable for old processes.
            self.db.execute("DROP TABLE files")
            self.db.execute("DROP TABLE packets")
            self.db.execute("DROP TABLE jobs")
            self.db.execute("PRAGMA user_version=2")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        self.version = 2
        return {"version": 2}

    def _blob_digest(self, table, rowid):
        hasher = hashlib.sha256()
        source = self.db.blobopen(table, "body", rowid, readonly=True)
        try:
            while True:
                chunk = source.read(CHUNK)
                if not chunk:
                    break
                hasher.update(chunk)
        finally:
            source.close()
        return hasher.hexdigest()

    def _blob_copy(self, src_table, src_rowid, dst_table, dst_rowid):
        source = self.db.blobopen(src_table, "body", src_rowid, readonly=True)
        target = self.db.blobopen(dst_table, "body", dst_rowid)
        try:
            while True:
                chunk = source.read(CHUNK)
                if not chunk:
                    break
                target.write(chunk)
        finally:
            source.close()
            target.close()

    # ----------------------------------------------------------------- advance
    async def advance(self, job, client, worker, now, ttl):
        """One SDK-2 step for a queued job: at most one pull per call."""
        if not isinstance(job, str) or not job:
            raise ValueError("INPUT")
        if not isinstance(worker, str) or not worker:
            raise ValueError("INPUT")
        if type(now) is not int or now < 0 or type(ttl) is not int or ttl < 1:
            raise ValueError("INPUT")
        if self.db.execute("PRAGMA user_version").fetchone()[0] != 2:
            raise ValueError("SCHEMA_UNSUPPORTED")
        kind, extra, state, request = self._acquire(job, now, ttl)
        if kind == "done":
            return {"job": job, "state": "done", "receipt": extra}
        if kind == "busy":
            return {"job": job, "state": "busy", "receipt": None}
        token = extra
        packet, revision, bind = request
        try:
            result = await self._advance_held(job, client, token, state,
                                              packet, revision, bind)
        except LeaseLost:
            return {"job": job, "state": "busy", "receipt": None}
        # Any other exception propagates and the lease is left to expire, so a
        # crashed or partitioned caller cannot be mistaken for the holder.
        self._release(job, token)
        return result

    def _acquire(self, job, now, ttl):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute("SELECT result FROM receipts WHERE job=?",
                                  (job,)).fetchone()
            if row:
                self.db.commit()
                return ("done", json.loads(row[0]), None, None)
            row = self.db.execute(
                "SELECT packet,revision,binding FROM queue WHERE job=?", (job,)).fetchone()
            if not row:
                self.db.rollback()
                raise ValueError("NO_JOB")
            packet, revision, bind = row
            transfer = self.db.execute(
                "SELECT consumer,session,manifest,next_cursor,pending_op,"
                "lease_until,failed FROM transfers WHERE job=?", (job,)).fetchone()
            if transfer and transfer[6] is not None:
                self.db.rollback()
                raise ValueError("CONTENT_MISMATCH")
            if transfer and transfer[5] is not None and transfer[5] > now:
                self.db.rollback()
                return ("busy", None, None, None)
            token = uuid.uuid4().hex
            consumer = canonical([self._archive_id(), job])
            if transfer:
                self.db.execute(
                    "UPDATE transfers SET lease_token=?, lease_until=? WHERE job=?",
                    (token, now + ttl, job))
                state = {"consumer": transfer[0], "session": transfer[1],
                         "manifest": json.loads(transfer[2]) if transfer[2] else None,
                         "next_cursor": transfer[3], "pending_op": transfer[4]}
            else:
                self.db.execute(
                    "INSERT INTO transfers(job,consumer,lease_token,lease_until) "
                    "VALUES(?,?,?,?)", (job, consumer, token, now + ttl))
                state = {"consumer": consumer, "session": None, "manifest": None,
                         "next_cursor": 0, "pending_op": None}
            self.db.commit()
            return ("ok", token, state, (packet, revision, bind))
        except BaseException:
            self.db.rollback()
            raise

    def _release(self, job, token):
        try:
            self.db.execute("BEGIN IMMEDIATE")
            self.db.execute(
                "UPDATE transfers SET lease_token=NULL, lease_until=NULL "
                "WHERE job=? AND lease_token=?", (job, token))
            self.db.commit()
        except Exception:
            self.db.rollback()

    def _check_lease(self, job, token):
        row = self.db.execute("SELECT lease_token FROM transfers WHERE job=?",
                              (job,)).fetchone()
        if not row or row[0] != token:
            raise LeaseLost()

    def _guarded(self, job, token, fn):
        """Run fn inside a write transaction that the lease token still owns."""
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self._check_lease(job, token)
            result = fn()
            self.db.commit()
            return result
        except BaseException:
            self.db.rollback()
            raise

    def _fail(self, job, token, reason):
        def mark():
            self.db.execute("UPDATE transfers SET failed=? WHERE job=?", (reason, job))
            return True
        self._guarded(job, token, mark)

    async def _advance_held(self, job, client, token, state, packet, revision, bind):
        session = state["session"]
        manifest = state["manifest"]
        if session is None:
            opened = await client.open(state["consumer"], packet, revision)
            manifest = self._check_manifest(opened, packet, revision)
            session = opened["session"]
            def save_session():
                self.db.execute(
                    "UPDATE transfers SET session=?, manifest=? WHERE job=?",
                    (session, canonical(manifest), job))
            self._guarded(job, token, save_session)
        files = manifest["files"]
        plan = segment_map(files)
        status = await client.status(session)
        if canonical(status["manifest"]) != canonical(manifest):
            self._fail(job, token, "MANIFEST_CHANGED")
            raise ValueError("CONTENT_MISMATCH")
        next_cursor = state["next_cursor"]
        pending_op = state["pending_op"]
        remote_cursor = status["cursor"]
        remote_pending = status["pending"]
        pulled = False
        if pending_op is not None:
            # We hold the segment bytes; the ack either never landed or its
            # response was lost.  Ack is idempotent, so just settle it.
            if remote_pending not in (None, pending_op) or remote_cursor != next_cursor:
                raise RuntimeError("SESSION_STATE_CONFLICT")
            await self._ack(client, session, pending_op)
            self._after_ack(job, token, files, plan, next_cursor - 1)
            pending_op = None
        elif remote_pending is not None:
            # The pull landed remotely but the response (or our persist) was
            # lost: re-fetch the still-unacked operation for identical bytes.
            if remote_cursor != next_cursor + 1:
                raise RuntimeError("SESSION_STATE_CONFLICT")
            seg = await client.pull(session, remote_pending, next_cursor)
            body = self._check_segment(job, token, seg, plan, next_cursor)
            self._store_segment(job, token, next_cursor, remote_pending, body)
            pulled = True
            await self._ack(client, session, remote_pending)
            self._after_ack(job, token, files, plan, next_cursor)
            next_cursor += 1
        elif remote_cursor != next_cursor:
            raise RuntimeError("SESSION_STATE_CONFLICT")
        total = len(plan)
        if next_cursor == total:
            receipt = self._publish(job, token, packet, revision, bind, files, plan)
            return {"job": job, "state": "done", "receipt": receipt}
        if pulled:
            return {"job": job, "state": "pending", "receipt": None}
        operation = "op-%d" % next_cursor
        seg = await client.pull(session, operation, next_cursor)
        body = self._check_segment(job, token, seg, plan, next_cursor)
        self._store_segment(job, token, next_cursor, operation, body)
        await self._ack(client, session, operation)
        self._after_ack(job, token, files, plan, next_cursor)
        next_cursor += 1
        if next_cursor == total:
            receipt = self._publish(job, token, packet, revision, bind, files, plan)
            return {"job": job, "state": "done", "receipt": receipt}
        return {"job": job, "state": "pending", "receipt": None}

    def _check_manifest(self, opened, packet, revision):
        manifest = opened["manifest"]
        if manifest["packet"] != packet or manifest["revision"] != revision:
            raise ValueError("WRONG_EXPORT")
        files = manifest["files"]
        paths = [f["path"] for f in files]
        if len(set(paths)) != len(paths) or paths != sorted(paths):
            raise ValueError("WRONG_EXPORT")
        for entry in files:
            if set(entry) != {"path", "media", "size", "sha256"}:
                raise ValueError("WRONG_EXPORT")
            if type(entry["size"]) is not int or entry["size"] < 0:
                raise ValueError("WRONG_EXPORT")
        return manifest

    def _check_segment(self, job, token, seg, plan, index):
        path, offset, length, _, last = plan[index]
        try:
            body = base64.b64decode(seg["body"], validate=True)
        except Exception:
            body = None
        if (body is None or seg.get("path") != path or seg.get("offset") != offset
                or seg.get("eof") != last or seg.get("cursor") != index
                or seg.get("next") != index + 1 or len(body) != length):
            self._fail(job, token, "SEGMENT_MISMATCH:" + path)
            raise ValueError("CONTENT_MISMATCH")
        return body

    async def _ack(self, client, session, operation):
        try:
            await client.ack(session, operation)
        except RemoteError as error:
            if str(error) != "NO_OPERATION":
                raise

    def _store_segment(self, job, token, seq, operation, body):
        def store():
            self.db.execute("INSERT INTO segments VALUES(?,?,?)", (job, seq, body))
            self.db.execute(
                "UPDATE transfers SET next_cursor=?, pending_op=? WHERE job=?",
                (seq + 1, operation, job))
        self._guarded(job, token, store)

    def _after_ack(self, job, token, files, plan, seg_index):
        _, _, _, file_index, last = plan[seg_index]
        def settle():
            self.db.execute("UPDATE transfers SET pending_op=NULL WHERE job=?", (job,))
            if last:
                return self._verify_file(job, files[file_index], plan, file_index)
            return True
        if not self._guarded(job, token, settle):
            raise ValueError("CONTENT_MISMATCH")

    def _verify_file(self, job, entry, plan, file_index):
        """Hash-check a fully received file and move it into objects.

        Runs inside the caller's write transaction; payload is streamed in
        CHUNK-sized reads.  On mismatch only the failure reason is persisted.
        """
        indexes = [i for i, item in enumerate(plan) if item[3] == file_index]
        first, last = indexes[0], indexes[-1]
        want_size = entry["size"]
        want = entry["sha256"]
        hasher = hashlib.sha256()
        total = 0
        for (body,) in self.db.execute(
                "SELECT body FROM segments WHERE job=? AND seq BETWEEN ? AND ? "
                "ORDER BY seq", (job, first, last)):
            hasher.update(body)
            total += len(body)
        if total != want_size or hasher.hexdigest() != want:
            self.db.execute("UPDATE transfers SET failed=? WHERE job=?",
                            ("CONTENT_MISMATCH:" + entry["path"], job))
            return False
        if not self.db.execute("SELECT 1 FROM objects WHERE digest=?",
                               (want,)).fetchone():
            if want_size:
                self.db.execute("INSERT INTO objects VALUES(?, zeroblob(?))",
                                (want, want_size))
                object_row = self.db.execute(
                    "SELECT rowid FROM objects WHERE digest=?", (want,)).fetchone()[0]
                target = self.db.blobopen("objects", "body", object_row)
                try:
                    for (body,) in self.db.execute(
                            "SELECT body FROM segments WHERE job=? AND seq BETWEEN ? "
                            "AND ? ORDER BY seq", (job, first, last)):
                        target.write(body)
                finally:
                    target.close()
            else:
                self.db.execute("INSERT INTO objects VALUES(?,?)", (want, b""))
        self.db.execute("INSERT INTO done_files VALUES(?,?,?,?)",
                        (job, entry["path"], entry["media"], want))
        self.db.execute("DELETE FROM segments WHERE job=? AND seq BETWEEN ? AND ?",
                        (job, first, last))
        return True

    def _publish(self, job, token, packet, revision, bind, files, plan):
        """Verify any remainder, then atomically publish refs + receipt."""
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute("SELECT result FROM receipts WHERE job=?",
                                  (job,)).fetchone()
            if row:
                self.db.commit()
                return json.loads(row[0])
            self._check_lease(job, token)
            for file_index, entry in enumerate(files):
                done = self.db.execute(
                    "SELECT 1 FROM done_files WHERE job=? AND path=?",
                    (job, entry["path"])).fetchone()
                if not done and not self._verify_file(job, entry, plan, file_index):
                    self.db.commit()  # persist the failure reason only
                    raise ValueError("CONTENT_MISMATCH")
            digest = manifest_digest(files)
            head = self.db.execute(
                "SELECT revision,digest FROM heads WHERE packet=?", (packet,)).fetchone()
            if head and head[0] == revision and head[1] != digest:
                raise ValueError("REVISION_CONFLICT")
            outcome = "superseded" if head and head[0] > revision else "published"
            result = {"job": job, "packet": packet, "revision": revision,
                      "outcome": outcome}
            if outcome == "published":
                self.db.execute("DELETE FROM refs WHERE packet=?", (packet,))
                for entry in files:
                    self.db.execute("INSERT INTO refs VALUES(?,?,?,?)",
                                    (packet, entry["path"], entry["media"],
                                     entry["sha256"]))
                self.db.execute("INSERT OR REPLACE INTO heads VALUES(?,?,?)",
                                (packet, revision, digest))
            self.db.execute("INSERT INTO receipts VALUES(?,?,?)",
                            (job, bind, canonical(result)))
            self.db.execute("DELETE FROM queue WHERE job=?", (job,))
            self.db.execute("DELETE FROM segments WHERE job=?", (job,))
            self.db.execute("DELETE FROM done_files WHERE job=?", (job,))
            self.db.execute("DELETE FROM transfers WHERE job=?", (job,))
            self.db.execute(
                "DELETE FROM objects WHERE digest NOT IN (SELECT digest FROM refs) "
                "AND digest NOT IN (SELECT digest FROM done_files)")
            self.db.commit()
            return result
        except BaseException:
            self.db.rollback()
            raise
