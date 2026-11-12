BEGIN TRANSACTION;
CREATE TABLE files(packet TEXT, path TEXT, media TEXT, body BLOB NOT NULL,
                   PRIMARY KEY(packet,path));
INSERT INTO "files" VALUES('R-17','主表.pdf','application/pdf',X'255044462D312E340A2525454F460A');
INSERT INTO "files" VALUES('R-17','附件/A.txt','text/plain',X'6162630A');
INSERT INTO "files" VALUES('R-17','附件/empty.txt','text/plain',X'');
CREATE TABLE jobs(job TEXT PRIMARY KEY, packet TEXT NOT NULL, revision INTEGER NOT NULL,
                  binding TEXT NOT NULL);
INSERT INTO "jobs" VALUES('pending-2','R-17',2,'0f61f0dd5d495276ffd4b0e5fd5b5d90952a307239620e74b0e4ad1beaf8047e');
CREATE TABLE meta(archive_id TEXT NOT NULL);
INSERT INTO "meta" VALUES('site-archive-2026');
CREATE TABLE packets(packet TEXT PRIMARY KEY, revision INTEGER NOT NULL, digest TEXT NOT NULL);
INSERT INTO "packets" VALUES('R-17',1,'d552f73d7d49847c6b6399173927d2f2c0bcfdef3ab3938020e1b30c936ebaa3');
CREATE TABLE receipts(job TEXT PRIMARY KEY, binding TEXT NOT NULL, result TEXT NOT NULL);
INSERT INTO "receipts" VALUES('old-1','53259d875a0d027831ece5184d0233662bc48bd3f93459b8e03ea001c72bd1be','{"job":"old-1","outcome":"published","packet":"R-17","revision":1}');
PRAGMA user_version=1;
COMMIT;
