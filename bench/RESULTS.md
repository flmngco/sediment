# sediment vs exqlite

`run.exs` runs the same operations on exqlite 0.41 (SQLite, WAL) and on
sediment (turso_core 0.8.1) in WAL and in MVCC journal mode, all with
`synchronous = NORMAL`, on a 10,000-row `users` table:

```sh
cd bench && mix deps.get && MIX_ENV=prod mix run run.exs
```

## Before/after the write-path work (2026-09-30)

Per operation, in microseconds: the median of three runs, each the best of
15 batches, before (8a4649d) and after, run alternately in the same session
with exqlite in the same VM as the reference: `MIX_ENV=prod mix run ab.exs`
(pools of one connection).

| Operation | exqlite (WAL) | sediment WAL before -> after | sediment MVCC before -> after |
| --- | --- | --- | --- |
| Pool: insert one row (`query/3`) | 39 | 111 -> **25** | 101 -> **30** |
| Pool: point select (`query/3`) | 45 | 71 -> **18** | 73 -> **25** |
| Raw: autocommit insert (`bind` + `step`) | 11 | 17 -> 16 | 17 -> 13 |
| Raw: insert inside a transaction | 3.5 | 6.6 -> 5.2 | 9.3 -> 9-12 (noise) |

* **Through DBConnection (and so Ecto) sediment is now faster than
  exqlite**, where it was 2.5-3x slower: a statement is one native call
  instead of six, and two per-query costs found by profiling (a regular
  expression and `Path.expand/1`, each run in both the prepare and the
  execute callback) are gone.

* **Raw autocommit inserts** are within 1.2-1.5x of exqlite (WAL) or on par
  (MVCC). turso_core itself commits faster than that (median 9 us WAL,
  6 us MVCC); the rest is two dirty-scheduler calls (`bind`, `step`) that
  the exqlite-compatible API requires.
* **Inserts inside a transaction** remain ~1.5-2x exqlite's per statement:
  turso_core spends 1.5-2.6 us per insert (SQLite less), plus the same two
  native calls.

Re-run on main ef6147f (with the async S3 work) at load ~3.3, median
of three `ab.exs` runs, in us: pool insert exqlite 35.6, WAL 20.9, MVCC 20.5;
pool point select 45.8 / 16.4 / 21.6; raw autocommit insert 8.2 / 11.5 / 13.4;
insert in a transaction 2.4 / 4.3 / 5.6. The same picture as the table.

The Benchee tables below are from 2026-09-29, under load average 13-17. The
same Benchee run could not be repeated reliably on 2026-09-30: the machine
ran at load 15-26, and throughput swung up to 100x between runs.

## Summary (2026-09-29, before the write-path work)

Measured on 2026-09-29 on a shared 8-core Linux VM under heavy unrelated
load (load average 13-17), so absolute numbers are noisy; the ratios are
more telling. Benchee, 1 s warmup, 3 s per scenario.

| Scenario | exqlite (WAL) | sediment (WAL) | sediment (MVCC) |
| --- | --- | --- | --- |
| Insert one row, autocommit, prepared | 13.6 K/s | 2.3 K/s | 5.6 K/s |
| Point select by primary key, prepared | 25.8 K/s | **56.4 K/s** | 48.9 K/s |
| Transaction of 1000 inserts | 73.6 /s | 28.4 /s | 17.0 /s |
| Select 1000 rows | 609 /s | **1,099 /s** | 821 /s |
| DBConnection pool: point select (`query/3`) | 16.5 K/s | 12.9 K/s | 13.9 K/s |
| 4 concurrent writers, 1000 transactions | 2,968 tx/s | 3,179 tx/s | **6,674 tx/s** |

* **Reads are faster** on sediment: prepared point selects about 2x and
  1000-row scans about 1.8x exqlite.
* **Writes are slower per statement**: turso_core's insert path costs more
  than SQLite's (a 1000-insert transaction takes 2.6x as long in WAL mode).
  Autocommit inserts in WAL mode have a long tail (99th percentile ~14 ms)
  from checkpoints.
* **Concurrent writers favour MVCC**: with `BEGIN CONCURRENT`, four writers
  commit about 2.2x as many transactions per second as exqlite, whose writers
  serialize on SQLite's lock.
* **Through DBConnection** sediment is within 1.2-1.3x of exqlite.
  `Sediment.Connection` caches prepared statements per connection because
  turso's prepare is much more expensive than SQLite's (before the cache the
  gap was 2.2-3.4x).

## Write path: where the time goes (2026-09-30)

The 2026-09-29 summary table showed autocommit inserts 2.4x (MVCC) to 6x
(WAL) slower than exqlite. To split the cost, `native/sediment_nif/examples/write_bench.rs`
runs the same workload straight on turso_core, with the calls the NIF
makes (reset, clear_bindings, bind_at, step and drive the IO):

```sh
cargo run --release --example write_bench -- wal 3     # or mvcc
```

On this machine the load from other jobs swings wall-clock times by 2-5x
between runs, so the numbers below are the best of 15 batches (robust
against interference) or CPU time, measured back to back.

### turso_core alone vs through the driver

| Per insert | turso_core | sediment low level (`bind` + `step`) |
| --- | --- | --- |
| WAL, inside a transaction | 1.5-2.0 us | 6-7 us |
| WAL, autocommit | 7.5-9 us (median 9 us, p99 40 us) | 13-17 us |
| MVCC, inside a transaction | 2.2-2.6 us | 9 us |
| MVCC, autocommit | 6.1 us (median 6-7 us, p99 20-25 us) | 14 us |

turso_core's own insert is fast: on the median it commits an autocommit
insert faster than exqlite does end to end. The rest is per-call overhead
on the Elixir side: each native call is a hop to a dirty IO scheduler
(0.9-2.8 us each depending on load), plus encoding the values. The step
loop itself costs nothing measurable: the NIF's `advance()` run in Rust
matches raw stepping (an ignored test in `stmt.rs` measures it), and pinning
everything to one dirty scheduler (`+SDio 1`) changes nothing, so thread
migration isn't a factor either.

### Removed overhead

* `Sediment.Connection` ran six native calls per statement
  (`bind_parameter_count`, `bind_all`, `multi_step`, `columns`,
  `transaction_status`, `changes`); `run_prepared` does it in one. CPU time
  per pooled insert (busy-waiting schedulers disabled, so CPU is real work):
  prepared `execute` 100-103 us -> 50-67 us, an insert inside a transaction
  70-74 us -> 42-48 us.
* `bind/2` checks the parameter count in the same native call (two calls
  -> one).
* A profile (`:tprof`) of pooled inserts showed a regular expression run on
  every query (the statement cache's "is this a PRAGMA?" check, now only on a
  cache miss and without a regex) and `Path.expand/1` on every query (the
  replica refresh check, now a key computed once per connection).

What remains per pooled query is mostly DBConnection itself (checkout, ETS
bookkeeping, timers), the same for exqlite.

### turso_core knobs

Measured with `write_bench` (knobs via environment variables, see the file):

| Knob | Effect |
| --- | --- |
| io_uring backend (`--features io_uring`, `IO=uring`) | Slower: WAL autocommit 27.7 K/s -> 18.3 K/s, MVCC 88 K/s -> 32.6 K/s, p99 3-4x worse. Small synchronous commits don't benefit; the syscall backend stays. |
| `synchronous = FULL` | An fsync per commit: 3-4 ms per autocommit insert on this disk. |
| `synchronous = NORMAL` (the default) | As in the tables above. |
| `synchronous = OFF`, WAL | Pathological in turso_core 0.8.1 (median 1.3 ms per autocommit insert); don't use it. |
| `wal_autocheckpoint` 100 / 1000 / 10000 pages | No significant difference. |
| MVCC checkpoint threshold | See below. |

turso_core's auto checkpoint is like SQLite's (passive, every 1000 WAL
frames, inside the committing statement, with fsyncs of the WAL and the
database file). The 14 ms p99 of the WAL autocommit row in the first table
does not reproduce on a quiet machine (p99 40 us on turso_core, 74 us
through the driver); it appears when other jobs load the disk, which makes
those fsyncs slow. Without perf or strace on this machine it could not be
attributed further.

### The MVCC checkpoint threshold

| Threshold | Autocommit | 1000-insert transaction |
| --- | --- | --- |
| 64 KiB | 14 K/s | 55 /s |
| 256 KiB (sediment's default) | 41-55 K/s | 80-126 /s |
| 1 MiB | 74 K/s | 141-148 /s |
| 4 MiB (turso_core's default) | 83-96 K/s | 182-192 /s |

Larger is faster for plain tables, at the price of longer pauses when a
checkpoint runs (up to ~230 ms at 4 MiB). But with an `AUTOINCREMENT` key
(Ecto's default primary key) every insert updates the same
`sqlite_sequence` row, and turso_core keeps a version per update until the
next checkpoint: autocommit inserts ran at 5,000/s with 256 KiB and 650/s
with 1 MiB. sediment keeps 256 KiB, which protects that common case;
set `mvcc_checkpoint_threshold:` higher for tables without
`AUTOINCREMENT`.

## Raw output

```
## Insert one row (autocommit, prepared)
Name                          ips        average  deviation         median         99th %
exqlite (WAL)             13.56 K       73.77 μs  ±1291.65%       23.38 μs      148.28 μs
sediment (MVCC)        5.57 K      179.67 μs   ±943.84%       47.48 μs     2818.84 μs
sediment (WAL)         2.25 K      443.89 μs   ±665.71%       60.71 μs    14115.33 μs
Comparison: 
exqlite (WAL)             13.56 K
sediment (MVCC)        5.57 K - 2.44x slower +105.89 μs
sediment (WAL)         2.25 K - 6.02x slower +370.11 μs
## Point select by primary key (prepared)
Name                          ips        average  deviation         median         99th %
sediment (WAL)        56.36 K       17.74 μs   ±703.19%        7.61 μs       80.50 μs
sediment (MVCC)       48.86 K       20.47 μs   ±738.91%        9.65 μs       76.23 μs
exqlite (WAL)             25.77 K       38.80 μs   ±592.83%       25.52 μs      146.91 μs
Comparison: 
sediment (WAL)        56.36 K
sediment (MVCC)       48.86 K - 1.15x slower +2.73 μs
exqlite (WAL)             25.77 K - 2.19x slower +21.06 μs
## Transaction of 1000 inserts
Name                          ips        average  deviation         median         99th %
exqlite (WAL)               73.55       13.60 ms    ±79.46%       12.74 ms       62.76 ms
sediment (WAL)          28.44       35.17 ms    ±58.26%       39.58 ms       93.65 ms
sediment (MVCC)         17.04       58.68 ms   ±115.01%       42.30 ms      403.95 ms
Comparison: 
exqlite (WAL)               73.55
sediment (WAL)          28.44 - 2.59x slower +21.57 ms
sediment (MVCC)         17.04 - 4.32x slower +45.09 ms
## Select 1000 rows
Name                          ips        average  deviation         median         99th %
sediment (WAL)        1099.06        0.91 ms    ±75.13%        0.88 ms        3.29 ms
sediment (MVCC)        821.29        1.22 ms   ±125.02%        1.01 ms        6.03 ms
exqlite (WAL)              609.45        1.64 ms   ±116.53%        1.41 ms        6.45 ms
Comparison: 
sediment (WAL)        1099.06
sediment (MVCC)        821.29 - 1.34x slower +0.31 ms
exqlite (WAL)              609.45 - 1.80x slower +0.73 ms
## DBConnection pool: point select (query/3)
Name                          ips        average  deviation         median         99th %
exqlite (WAL)             16.48 K       60.68 μs   ±251.18%       42.40 μs      279.50 μs
sediment (MVCC)       13.93 K       71.79 μs   ±326.05%       42.60 μs      351.95 μs
sediment (WAL)        12.90 K       77.49 μs   ±285.47%          59 μs      259.38 μs
Comparison: 
exqlite (WAL)             16.48 K
sediment (MVCC)       13.93 K - 1.18x slower +11.11 μs
sediment (WAL)        12.90 K - 1.28x slower +16.82 μs
## 4 concurrent writers, 1000 single-row transactions in total
exqlite (WAL)          336 ms (2968 tx/s)
sediment (MVCC)    149 ms (6674 tx/s)
sediment (WAL)     314 ms (3179 tx/s)
```
