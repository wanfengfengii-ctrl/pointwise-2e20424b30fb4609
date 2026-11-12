"""Working legacy archive. It intentionally has no SDK-2 or schema-2 adapter."""
import base64
import hashlib
import json
import sqlite3
import uuid
from portal import canonical

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

def binding(packet, revision):
    return hashlib.sha256(canonical([packet, revision]).encode()).hexdigest()

def package_digest(files):
    manifest = []
    for item in sorted(files, key=lambda x: x["path"]):
        body = base64.b64decode(item["body"], validate=True)
        manifest.append({"path": item["path"], "media": item["media"],
                         "size": len(body), "sha256": hashlib.sha256(body).hexdigest()})
    return hashlib.sha256(canonical(manifest).encode()).hexdigest()

class Archive:
    def __init__(self, db):
        self.db = db
        self.db.execute("PRAGMA busy_timeout=5000")
        version = self.db.execute("PRAGMA user_version").fetchone()[0]
        if version == 0:
            self.db.executescript(DDL)
            with self.db:
                self.db.execute("INSERT INTO meta VALUES(?)", (uuid.uuid4().hex,))
        elif version != 1:
            raise ValueError("SCHEMA_UNSUPPORTED")

    def enqueue(self, job, packet, revision):
        if not all(isinstance(x, str) and x for x in (job, packet)):
            raise ValueError("INPUT")
        if type(revision) is not int or revision < 1:
            raise ValueError("INPUT")
        digest = binding(packet, revision)
        with self.db:
            old = self.db.execute("SELECT binding,result FROM receipts WHERE job=?", (job,)).fetchone()
            if old:
                if old[0] != digest:
                    raise ValueError("JOB_CONFLICT")
                return json.loads(old[1])
            old = self.db.execute("SELECT binding FROM jobs WHERE job=?", (job,)).fetchone()
            if old and old[0] != digest:
                raise ValueError("JOB_CONFLICT")
            self.db.execute("INSERT OR IGNORE INTO jobs VALUES(?,?,?,?)",
                            (job, packet, revision, digest))
        return {"job": job, "outcome": "pending"}

    def run(self, job, client, fail_at=None):
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

    def read(self):
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
