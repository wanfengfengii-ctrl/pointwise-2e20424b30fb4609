import argparse
import asyncio
import json
import sqlite3
import time
from archive import Archive
from portal import Portal
from vendor.export_v1 import Client as V1
from vendor.export_v2 import Client as V2

def load_source(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)

async def drive(archive, job, client):
    while True:
        result = await archive.advance(job, client, "work", int(time.time()), 5)
        if result["state"] == "done":
            return result["receipt"]
        if result["state"] == "busy":
            await asyncio.sleep(0.2)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    sub = parser.add_subparsers(dest="action", required=True)
    queue = sub.add_parser("enqueue")
    queue.add_argument("job")
    queue.add_argument("packet")
    queue.add_argument("revision", type=int)
    run = sub.add_parser("work")
    run.add_argument("job")
    run.add_argument("source")
    sub.add_parser("read")
    sub.add_parser("migrate")
    step = sub.add_parser("step")
    step.add_argument("job")
    step.add_argument("source")
    step.add_argument("--remote", required=True)
    step.add_argument("--worker", required=True)
    step.add_argument("--now", required=True, type=int)
    step.add_argument("--ttl", required=True, type=int)
    args = parser.parse_args()
    db = sqlite3.connect(args.db)
    remote_db = None
    try:
        archive = Archive(db)
        if args.action == "enqueue":
            result = archive.enqueue(args.job, args.packet, args.revision)
        elif args.action == "read":
            result = archive.read()
        elif args.action == "migrate":
            result = archive.migrate()
        elif args.action == "step":
            archive.migrate()  # atomic switch; idempotent
            remote_db = sqlite3.connect(args.remote, check_same_thread=False)
            portal = Portal(load_source(args.source), remote_db, v1=False)
            client = V2(transport=portal.dispatch)
            result = asyncio.run(archive.advance(
                args.job, client, args.worker, args.now, args.ttl))
        else:  # work
            if archive.version == 2:
                remote_db = sqlite3.connect(args.db + ".remote.sqlite",
                                            check_same_thread=False)
                portal = Portal(load_source(args.source), remote_db, v1=False)
                result = asyncio.run(drive(archive, args.job,
                                           V2(transport=portal.dispatch)))
            else:
                remote = Portal(load_source(args.source))
                result = archive.run(args.job, V1(transport=remote.dispatch))
                remote.db.close()
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    except Exception as error:
        print(json.dumps({"error": str(error)}, ensure_ascii=False))
        raise SystemExit(2)
    finally:
        if remote_db is not None:
            remote_db.close()
        db.close()

if __name__ == "__main__":
    main()
