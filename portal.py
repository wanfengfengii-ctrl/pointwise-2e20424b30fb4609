"""Executable supplier service, including persistent single-use export cursors."""
import base64
import hashlib
import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

CHUNK = 65536

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)

class Portal:
    def __init__(self, source, connection=None, v1=True):
        self.db = connection or sqlite3.connect(":memory:", check_same_thread=False)
        self.lock = threading.RLock()
        self.v1 = v1
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS snapshots(
          packet TEXT, revision INTEGER, manifest TEXT, pages TEXT,
          PRIMARY KEY(packet,revision));
        CREATE TABLE IF NOT EXISTS sessions(
          id TEXT PRIMARY KEY, consumer TEXT UNIQUE, packet TEXT,
          revision INTEGER, cursor INTEGER NOT NULL, pending TEXT);
        CREATE TABLE IF NOT EXISTS operations(
          session TEXT, operation TEXT, cursor INTEGER, response TEXT,
          acked INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(session,operation));
        CREATE TABLE IF NOT EXISTS traffic(
          seq INTEGER PRIMARY KEY, action TEXT, request TEXT, result TEXT);
        """)
        with self.db:
            for item in source["packets"]:
                manifest, pages = [], []
                for entry in sorted(item["files"], key=lambda x: x["path"]):
                    raw = base64.b64decode(entry["body"], validate=True)
                    meta = {"path": entry["path"], "media": entry["media"],
                            "size": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
                    manifest.append(meta)
                    starts = range(0, len(raw), CHUNK) if raw else [0]
                    for offset in starts:
                        part = raw[offset:offset + CHUNK]
                        pages.append({"path": entry["path"], "offset": offset,
                                      "body": base64.b64encode(part).decode(),
                                      "eof": offset + len(part) == len(raw)})
                view = {"packet": item["packet"], "revision": item["revision"],
                        "files": manifest}
                old = self.db.execute(
                    "SELECT manifest,pages FROM snapshots WHERE packet=? AND revision=?",
                    (item["packet"], item["revision"])).fetchone()
                encoded = (canonical(view), canonical(pages))
                if old and tuple(old) != encoded:
                    raise ValueError("IMMUTABLE_SOURCE_CONFLICT")
                self.db.execute("INSERT OR IGNORE INTO snapshots VALUES(?,?,?,?)",
                                (item["packet"], item["revision"], *encoded))

    def dispatch(self, action, request):
        with self.lock:
            try:
                self.db.execute("BEGIN IMMEDIATE")
                result = self._dispatch(action, request)
                self.db.execute("INSERT INTO traffic(action,request,result) VALUES(?,?,?)",
                                (action, canonical(request), canonical({
                                    "keys": sorted(result), "cursor": result.get("cursor")})))
                self.db.commit()
                return result
            except (ValueError, KeyError) as error:
                self.db.rollback()
                return {"error": str(error.args[0])}
            except BaseException:
                self.db.rollback()
                raise

    def snapshot(self, packet, revision):
        row = self.db.execute(
            "SELECT manifest,pages FROM snapshots WHERE packet=? AND revision=?",
            (packet, revision)).fetchone()
        if not row:
            raise ValueError("NO_SNAPSHOT")
        return json.loads(row[0]), json.loads(row[1])

    def _dispatch(self, action, p):
        if action == "v1/export":
            if not self.v1:
                raise ValueError("V1_RETIRED")
            manifest, pages = self.snapshot(p["packet"], p["revision"])
            files = []
            for meta in manifest["files"]:
                raw = b"".join(base64.b64decode(x["body"]) for x in pages
                               if x["path"] == meta["path"])
                files.append({"path": meta["path"], "media": meta["media"],
                              "body": base64.b64encode(raw).decode()})
            return {**manifest, "files": files}
        if action == "v2/open":
            manifest, _ = self.snapshot(p["packet"], p["revision"])
            old = self.db.execute("SELECT * FROM sessions WHERE consumer=?",
                                  (p["consumer"],)).fetchone()
            if old:
                if (old[2], old[3]) != (p["packet"], p["revision"]):
                    raise ValueError("CONSUMER_CONFLICT")
                session = old[0]
            else:
                session = uuid.uuid4().hex
                self.db.execute("INSERT INTO sessions VALUES(?,?,?,?,0,NULL)",
                                (session, p["consumer"], p["packet"], p["revision"]))
            return {"session": session, "manifest": manifest}
        row = self.db.execute("SELECT * FROM sessions WHERE id=?",
                              (p["session"],)).fetchone()
        if not row:
            raise ValueError("NO_SESSION")
        session, _, packet, revision, cursor, pending = row
        manifest, pages = self.snapshot(packet, revision)
        if action == "v2/status":
            return {"cursor": cursor, "pending": pending,
                    "done": cursor == len(pages) and pending is None,
                    "manifest": manifest}
        if action == "v2/pull":
            old = self.db.execute(
                "SELECT cursor,response,acked FROM operations WHERE session=? AND operation=?",
                (session, p["operation"])).fetchone()
            if old:
                if old[0] != p["cursor"]:
                    raise ValueError("OPERATION_CONFLICT")
                if old[2]:
                    raise ValueError("ACKED")
                return json.loads(old[1])
            if pending is not None:
                raise ValueError("UNACKED")
            if p["cursor"] != cursor:
                raise ValueError("CURSOR_CONFLICT")
            if cursor == len(pages):
                raise ValueError("END")
            result = {**pages[cursor], "cursor": cursor, "next": cursor + 1}
            self.db.execute("INSERT INTO operations VALUES(?,?,?,?,0)",
                            (session, p["operation"], cursor, canonical(result)))
            self.db.execute("UPDATE sessions SET cursor=?,pending=? WHERE id=?",
                            (cursor + 1, p["operation"], session))
            return result
        if action == "v2/ack":
            old = self.db.execute(
                "SELECT acked FROM operations WHERE session=? AND operation=?",
                (session, p["operation"])).fetchone()
            if not old:
                raise ValueError("NO_OPERATION")
            if not old[0]:
                self.db.execute(
                    "UPDATE operations SET acked=1,response=NULL WHERE session=? AND operation=?",
                    (session, p["operation"]))
                self.db.execute("UPDATE sessions SET pending=NULL WHERE id=?", (session,))
            return {"acked": p["operation"]}
        raise ValueError("NO_ENDPOINT")

@contextmanager
def serving(portal, host="127.0.0.1", port=0):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            try:
                count = int(self.headers["Content-Length"])
                request = json.loads(self.rfile.read(count))
                result = portal.dispatch(self.path.lstrip("/"), request)
                payload = canonical(result).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer((host, port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield "http://%s:%s" % server.server_address
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
