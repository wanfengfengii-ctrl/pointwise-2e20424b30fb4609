import argparse
import json
import sqlite3
from archive import Archive
from portal import Portal
from vendor.export_v1 import Client

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
    args = parser.parse_args()
    db = sqlite3.connect(args.db)
    try:
        archive = Archive(db)
        if args.action == "enqueue":
            result = archive.enqueue(args.job, args.packet, args.revision)
        elif args.action == "read":
            result = archive.read()
        else:
            with open(args.source, encoding="utf-8") as handle:
                source = json.load(handle)
            remote = Portal(source)
            result = archive.run(args.job, Client(transport=remote.dispatch))
            remote.db.close()
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    except Exception as error:
        print(json.dumps({"error": str(error)}, ensure_ascii=False))
        raise SystemExit(2)
    finally:
        db.close()

if __name__ == "__main__":
    main()
