`sqlite_app.db`: written by SQLite (Python's sqlite3, rollback-journal mode), for the import
tests. It has AUTOINCREMENT tables whose top rows were deleted (`posts`: ids 1-3, sequence 5),
emptied AUTOINCREMENT tables: with a NOT NULL column (`empty_seq`, sequence 2), without one
(`empty_nullable`, sequence 3), Ecto-like with NOT NULL timestamps and a UNIQUE blob (`sessions`,
sequence 2), and one whose CHECK refuses placeholder values (`empty_checked`, sequence 1), an
Ecto `users` table with a UNIQUE email whose newest row was deleted (ids 1-2, sequence 3), a
foreign key with ON DELETE CASCADE, a CHECK constraint, a unique and a plain index, a view, an
AFTER INSERT trigger, a rowid table with a gap (`plain`, rowids 2 and 3), and user_version 7.
`make_sqlite_app.py` regenerates it.
