# Regenerates sqlite_app.db with SQLite itself (python3 make_sqlite_app.py); see README.md.
import os
import sqlite3

if os.path.exists("sqlite_app.db"):
    os.remove("sqlite_app.db")
c = sqlite3.connect("sqlite_app.db")
c.executescript("""
CREATE TABLE authors (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE);
CREATE TABLE posts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  author_id INTEGER NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
  title TEXT NOT NULL CHECK (title != ''),
  body BLOB,
  score REAL DEFAULT 0
);
CREATE INDEX posts_author ON posts (author_id);
CREATE TABLE audit (msg TEXT NOT NULL);
CREATE TABLE empty_seq (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT NOT NULL);
CREATE TABLE plain (k TEXT, v INTEGER);
CREATE VIEW post_titles AS SELECT p.id, a.name, p.title FROM posts p JOIN authors a ON a.id = p.author_id;
CREATE TRIGGER posts_audit AFTER INSERT ON posts BEGIN INSERT INTO audit VALUES ('post ' || new.title); END;
INSERT INTO authors (name) VALUES ('ann'), ('bob');
INSERT INTO posts (author_id, title, body, score) VALUES (1, 'one', x'00ff', 1.5), (2, 'two', NULL, 2.5),
  (1, 'three', x'01', 3), (1, 'four', NULL, 4), (2, 'five', NULL, 5);
DELETE FROM posts WHERE id IN (4, 5);
INSERT INTO empty_seq (v) VALUES ('x'), ('y');
DELETE FROM empty_seq;
INSERT INTO plain VALUES ('a', 1), ('b', 2), ('c', 3);
DELETE FROM plain WHERE k = 'a';
CREATE TABLE empty_nullable (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO empty_nullable (v) VALUES ('x'), ('y'), ('z');
DELETE FROM empty_nullable;
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL UNIQUE,
  inserted_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
INSERT INTO users (email, inserted_at, updated_at) VALUES
  ('a@x', '2026-01-01T00:00:00', '2026-01-01T00:00:00'),
  ('b@x', '2026-01-02T00:00:00', '2026-01-02T00:00:00'),
  ('c@x', '2026-01-03T00:00:00', '2026-01-03T00:00:00');
DELETE FROM users WHERE email = 'c@x';
CREATE TABLE sessions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  token BLOB NOT NULL UNIQUE,
  user_id INTEGER NOT NULL,
  inserted_at TEXT NOT NULL
);
INSERT INTO sessions (token, user_id, inserted_at) VALUES (x'01', 1, 't'), (x'02', 2, 't');
DELETE FROM sessions;
CREATE TABLE empty_checked (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT NOT NULL CHECK (length(v) > 3));
INSERT INTO empty_checked (v) VALUES ('long enough');
DELETE FROM empty_checked;
PRAGMA user_version = 7;
""")
c.commit()
c.close()
