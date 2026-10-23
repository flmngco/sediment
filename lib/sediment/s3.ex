defmodule Sediment.S3 do
  @moduledoc """
  S3-backed durability (sediment extension).

  Opening a database with the `:s3` option makes an S3 bucket/prefix its
  durable state of record. The local file is a working copy:

      {:ok, db} =
        Sediment.Engine.open("/var/lib/app/app.db",
          s3: [
            bucket: "my-bucket",
            prefix: "prod/app",
            region: "eu-central-1",
            access_key_id: "...",
            secret_access_key: "..."
          ],
          encryption: [cipher: "aegis256", key: System.fetch_env!("DATABASE_ENCRYPTION_KEY")]
        )

  * On open, the database is restored from S3 (latest snapshot plus the
    logical log), or created in S3 when the prefix is empty.
    **A restore replaces any existing local files at the path.** When the
    prefix is empty, an open over a local database with tables fails with
    `"s3 config: ... refusing to replace the local file ..."` and leaves
    it alone; `import/3` uploads an existing database instead.
  * Commits are stored in S3 in commit order. With `durability: :async`
    (the default) a background uploader stores them shortly after each
    commit returns; `sync: true` on a transaction or statement, or
    `flush/2`, waits for it. With `durability: :sync` every commit returns
    only once stored, and a failed upload fails and rolls back the commit.
    Async commits lost before their upload (a fenced writer, a close that
    couldn't reach S3) make `flush/2` and `sync: true` fail until
    `acknowledge_loss/1`. See the S3 guide's "Durability" section.
  * A checkpoint (automatic, or `PRAGMA wal_checkpoint(TRUNCATE)`, or
    `snapshot/1`) starts a new log epoch; a snapshot of the database file
    (by default only the 64 KiB segments that changed) is uploaded in the
    background right after, and older objects are deleted. `snapshot/1`
    waits for the upload.
  * One writer at a time: the opener takes a lease on `<prefix>/lease.json`
    and renews it in the background. A second writer fails with
    `"s3 lease held by ..."` until the lease is released (on close) or
    expires. A writer that loses the lease is fenced: its commits fail.

  The database always runs in MVCC journal mode (`journal_mode: :mvcc`).
  In-memory databases are not supported with `:s3`.

  ## Encryption

  S3 databases are encrypted by default: every open with `:s3`, `restore/3`
  and `import/3` needs either an `:encryption` key
  (`[cipher: "aegis256", key: <64 hex chars>]`, see `generate_key/0`) or
  `encryption: false`, the explicit choice to store the database
  unencrypted. Without either they fail with
  `"s3 config: S3 databases are encrypted by default: ..."` (or, when the
  prefix holds a database, a message saying whether it is encrypted).

  An encrypted S3 database is encrypted at rest in the bucket: snapshots
  are encrypted database files and log frames are encrypted by turso's
  logical log. Restoring (a writer's open, a replica, `restore/3`) needs the
  same key; a wrong or missing key fails the open.

  `:encryption` is a key ring. `[cipher: c, key: hex]` is a ring of one key
  whose id is `"default"`. A ring of several keys names each:

      encryption: [keys: [{"r2", key2}, {"r1", key1}], cipher: "aegis256"]

  The first key is the active one: new databases are created under it, and
  `rekey/3` moves databases to it. An entry may give its own cipher,
  `{"r1", key1, cipher: "aegis256"}` (`"aegis256"` by default). Ids match
  `[a-z0-9][a-z0-9_-]{0,31}`; ids and keys must be unique in a ring.

  Keys can also come from a function, called only with an id the database
  needs:

      encryption: [key_for: &MyApp.Keys.database_key/1, active: "r2"]

  `key_for` (a 1-arity function or `{module, function, args}`, called with
  the id first) returns the hex key, `{:ok, hex}`, or `nil` for an id it
  doesn't know. It may be combined with `keys:`.

  The manifest records, for the current epoch and every retained one, the
  id of its key, its cipher and its fingerprint (the first 16 bytes of
  HMAC-SHA256(key, "sediment/key-fingerprint/v1"), which tells keys apart
  without revealing them). An open takes the key from its ring by that id,
  after taking the lease and before writing anything, and refuses
  `"unknown key id r1 (ring: r2, r3)"`, or `"key r1 in the ring is not the
  key that encrypted this database (fingerprint ..., recorded ...)"` when the
  ring holds another key under the id. Errors never contain key material.
  See "Rotating keys" in the S3 guide for `rekey/3`, `release_key/4` and
  `key_census/1`.

  ## Read-only replicas

  `mode: :replica` opens a read-only copy of the database instead: it is
  restored from the current S3 state without taking the lease and never
  writes to S3, so any number of replicas can run next to the writer.

      {:ok, replica} =
        Sediment.Engine.open("/var/lib/app/replica.db",
          s3: [bucket: "my-bucket", prefix: "prod/app", mode: :replica],
          encryption: encryption
        )

  Writes fail with `"attempt to write a readonly database"`. The copy does
  not follow the writer by itself: call `refresh/1` to fetch the latest
  state. A replica's path must differ from the writer's.

  ## Options

  The most used ones; every option, with its default, is in the S3 guide's
  "Configuration" table.

    * `:bucket` - required. `:prefix` - key prefix for this database (default `""`).
    * `:endpoint` - for S3-compatible servers, e.g. `"http://127.0.0.1:8333"`
      (implies path-style URLs). `:region` - default `"us-east-1"`.
    * `:access_key_id`, `:secret_access_key`, `:session_token` - credentials;
      when omitted, the standard `AWS_*` environment variables are used.
    * `:durability` - `:async` (default): commits return once local and a
      background uploader stores them in order; `:sync`: every commit waits
      for its upload.
    * `:max_lag_ms` (default `1_000`) and `:max_pending_bytes` (default 16 MiB) -
      async: new commits wait while the oldest un-uploaded commit is older, or
      more log is not yet uploaded.
    * `:upload_interval_ms` (default `0`) - async: how long committed frames
      collect before they are uploaded as one segment. Fewer requests, a
      longer loss window on a crash; flushes, `sync: true` commits,
      checkpoints and close don't wait for it. Must be below `:max_lag_ms`.
    * `:close_timeout_ms` - async: how long closing waits for pending uploads,
      default `10_000`.
    * `:mode` - `:writer` (default) or `:replica`.

  See `docs/s3-durability.md` for the design.
  """

  require Logger

  alias Sediment.Native
  alias Sediment.S3.Keys
  alias Sediment.Telemetry

  @type info :: %{
          epoch: String.t(),
          snapshot: String.t(),
          generation: non_neg_integer(),
          owner: String.t(),
          lease_expires_at_ms: non_neg_integer(),
          log_offset: non_neg_integer(),
          uploaded_frames: non_neg_integer(),
          uploaded_objects: non_neg_integer(),
          uploaded_bytes: non_neg_integer(),
          snapshot_pending: boolean(),
          poisoned: String.t() | nil,
          durability: String.t(),
          pending_bytes: non_neg_integer(),
          pending_frames: non_neg_integer(),
          lag_ms: non_neg_integer(),
          durable_epoch: String.t(),
          durable_offset: non_neg_integer(),
          committed_offset: non_neg_integer(),
          lost: loss() | nil
        }

  @typedoc "Commits acknowledged and then lost: durable up to one position, committed up to another."
  @type loss :: %{
          durable_epoch: String.t(),
          durable_offset: non_neg_integer(),
          committed_epoch: String.t(),
          committed_offset: non_neg_integer()
        }

  @typedoc """
  The `:encryption` option: a key ring (see "Encryption"), or `false` for
  an unencrypted database.
  """
  @type encryption ::
          [cipher: String.t(), key: String.t()]
          | [
              keys: [{String.t(), String.t()} | {String.t(), String.t(), [cipher: String.t()]}],
              cipher: String.t()
            ]
          | [
              key_for: (String.t() -> String.t() | {:ok, String.t()} | nil) | mfa(),
              active: String.t(),
              keys: list(),
              cipher: String.t()
            ]
          | false

  @type replica_info :: %{
          mode: :replica,
          epoch: String.t(),
          snapshot: String.t(),
          log_bytes: non_neg_integer(),
          writer: String.t(),
          restored_at_ms: non_neg_integer()
        }

  @doc """
  Returns the S3 state of a database opened with the `:s3` option.

  For a replica, returns what it was last restored from.
  """
  @spec info(Sediment.Engine.db() | DBConnection.conn()) ::
          {:ok, info() | replica_info()} | {:error, term()}
  def info(db) when is_reference(db) do
    case Native.s3_info(db) do
      {:error, "not an s3 database"} -> Native.s3_replica_info(db)
      other -> other
    end
  end

  def info(conn), do: pool_call(conn, :info)

  @doc """
  Brings a replica up to date with the latest state in S3.

  Downloads what changed since the last refresh (the new log objects, or
  after a checkpoint the new snapshot deltas and log), rebuilds the
  replica's local copy and reconnects it. Statements prepared before the
  refresh are finalized and must be prepared again. When nothing changed in
  S3, the local copy is kept. Other connections to the same path keep the
  state they had until they are refreshed themselves.

  While another writer is taking the database over (until it sealed the old
  writer's log), the refresh returns an error at once and the replica keeps
  its state; the next refresh tries again (see "Replicas" in the S3 guide).

  Returns `{:error, "not an s3 replica"}` for other databases.

  After a rekey the replica switches to the new key at its next refresh:
  its ring must hold it. A ring with `key_for` resolves it when the
  `:encryption` option is given here again (a pool passes its own).

  With a `DBConnection` pool (for example a replica repo), the connection
  that runs the call refreshes at once and every other connection of the
  pool refreshes before its next statement outside a transaction.
  """
  @spec refresh(Sediment.Engine.db() | DBConnection.conn(), keyword()) ::
          {:ok, replica_info()} | {:error, term()}
  def refresh(db, opts \\ [])

  def refresh(db, opts) when is_reference(db) do
    Telemetry.span([:s3, :refresh], %{}, fn ->
      case Keys.normalize(opts[:encryption]) do
        %{key_for: key_for} = ring when key_for != nil ->
          Keys.run(ring, &Native.s3_refresh(db, &1))

        _ ->
          db |> Native.s3_refresh(nil) |> key_needed()
      end
    end)
  end

  def refresh(conn, _opts), do: pool_call(conn, :refresh)

  # A key a ring with key_for would have resolved, asked of one without.
  defp key_needed({:error, {:key_needed, id}}),
    do: {:error, "s3 config: needs key #{id}, which is not in the ring"}

  defp key_needed(other), do: other

  @doc """
  Checkpoints the database and uploads a snapshot to S3 now, instead of
  waiting for the next automatic checkpoint.
  """
  @spec snapshot(Sediment.Engine.db() | DBConnection.conn()) :: :ok | {:error, term()}
  def snapshot(db) when not is_reference(db), do: pool_call(db, :snapshot)

  def snapshot(db) do
    Telemetry.span([:s3, :snapshot], %{}, fn ->
      with :ok <- Sediment.Engine.execute(db, "PRAGMA wal_checkpoint(TRUNCATE)"),
           {:ok, _uploaded} <- Native.s3_flush_snapshot(db) do
        :ok
      end
    end)
  end

  @doc """
  Restores the database stored at the S3 location in `s3_opts` into a
  standalone file at `path`, without taking the lease or writing to S3.

  Point-in-time restore works over the epochs the writer retains
  (`retain_epochs: n`, plus those a rekey kept, see "Rotating keys" in the
  S3 guide): each epoch is a snapshot plus its full log, and covers the
  time from its start to the next epoch's start, both taken from the
  store's clock (the Last-Modified of the seal that closed the epoch
  before). A moment within the same second as a boundary (the store's
  resolution) restores the newer epoch. A moment no retained epoch covers
  is refused (`"time ... is not covered by a retained epoch (retained:
  [...])"`), never answered with another epoch's state.

  The restored epoch is read with the key its manifest record names, from
  the `:encryption` ring: `{:error, "s3 config: needs key r1, which is not
  in the ring"}` without it. The restored file stays encrypted with that
  key, which the result names (`:key`).

  Like `refresh/1`, it returns an error while a writer is taking the
  database over, until that writer sealed the old writer's log.

  ## Options

    * `:at` - a `DateTime`: the state as of that moment.
    * `:epoch` - an epoch sequence number: the state at the start of that
      epoch plus its whole log.
    * `:encryption` - the key ring of an encrypted database (as its
      `:encryption` open option, holding the key of the epoch restored), or
      `false` for an unencrypted one. Required, see "Encryption".

  Without options, the latest state is restored. Returns `{:ok, info}` with
  the epoch used, its key's id (`nil` when unencrypted) and how many log
  frames were replayed.

  `path` must not exist, and neither may its WAL (`path-wal`) or MVCC log
  (`<path without extension>.db-log`): restore returns
  `{:error, "restore target exists: ..."}` rather than replace a file, which
  could be another database or the working copy of a running writer. Delete
  them first to restore over an old copy.
  """
  @spec restore(Path.t(), keyword() | map(), keyword()) :: {:ok, map()} | {:error, term()}
  def restore(path, s3_opts, opts \\ []) do
    path = to_string(path)

    case Enum.find([path, path <> "-wal", Path.rootname(path) <> ".db-log"], &File.exists?/1) do
      nil -> do_restore(path, s3_opts, opts)
      existing -> {:error, "restore target exists: #{existing}"}
    end
  end

  defp do_restore(path, s3_opts, opts) do
    target =
      cond do
        at = opts[:at] -> {:time, DateTime.to_unix(at, :millisecond)}
        epoch = opts[:epoch] -> {:epoch, epoch}
        true -> :latest
      end

    s3_opts = with_encryption_choice(s3_opts, opts[:encryption])

    Telemetry.span([:s3, :restore], %{path: path}, fn ->
      Keys.run(
        Keys.normalize(opts[:encryption]),
        &Native.s3_restore(path, s3_opts, target, &1)
      )
    end)
  end

  @doc """
  Whether the S3 location in `s3_opts` holds a database: `false` when it
  holds none yet or one was destroyed. It reads `manifest.json` only: no
  lease, no writes. Raises `Sediment.Error` when the store can't be read
  (a store error is not an answer).

  Opening a location without a database creates a new, empty one. Where
  that must not happen, for example for a tenant you know exists, open with
  `must_exist: true` in the `s3` options instead of checking first: the
  open then fails with `"... no database at s3://... (must_exist: true)"`
  and writes nothing.
  """
  @spec exists?(keyword() | map()) :: boolean()
  def exists?(s3_opts) do
    case Keys.scrubbed(fn -> Native.s3_exists(s3_opts) end) do
      {:ok, exists} -> exists
      {:error, message} -> raise Sediment.Error, message: message
    end
  end

  @doc """
  Destroys the database stored at the S3 location in `s3_opts`: afterwards
  no open, replica or restore finds it, and its snapshots and log are
  deleted. Opening the location again creates a new, empty database.

  Stop every writer of the database first: like an open, destroy takes the
  writer lease, and it returns `{:error, "s3 lease held by ..."}` while a
  writer holds it (also one with this VM's owner name), or
  `{:error, "... is open in this VM ..."}` while a connection of this VM has
  the database open. Connections that are closing (a pool that just stopped)
  are waited for, up to their `:close_timeout_ms` and a second. A writer that
  was closed released the lease; one that crashed holds it until
  `lease_ttl_ms` has passed.

  The database is gone as soon as destroy replaces `manifest.json` with a
  marker that there is no database, before it deletes anything: a destroy
  that fails or is interrupted after that leaves objects behind, never an
  older state of the database. Run it again to delete them (an open does
  too). Two small objects stay at the prefix, holding no data: the marker
  and `lease.json`. They keep a later database at this location apart from
  any delayed write of the destroyed one, so don't delete them while a
  writer of the old database may still be running.

  The local files of writers and replicas are not touched; delete them
  yourself. An open that finds a local copy with tables over a destroyed
  database fails rather than replace it with the new, empty one.

  ## Options

    * `:force` - take the lease even while a writer holds it. That writer
      is fenced: it acknowledges no further commit, and commits it has not
      uploaded yet are lost with the database. Use it for a writer that
      can't be stopped, not instead of waiting for a crashed one's lease.
      It never overrides "open in this VM": close the database here first.

  Returns `{:ok, %{objects: n}}` with the number of objects deleted, or
  `{:error, message}`.
  """
  @spec destroy(keyword() | map(), keyword()) :: {:ok, map()} | {:error, term()}
  def destroy(s3_opts, opts \\ []) do
    Telemetry.span([:s3, :destroy], %{}, fn ->
      force = boolean!(opts, :force, "destroy/2")
      Keys.scrubbed(fn -> Native.s3_destroy(s3_opts, force) end)
    end)
  end

  @doc """
  Imports the existing database file at `path` (written by SQLite or
  Sediment) into the empty S3 location in `s3_opts`, as a new database.
  Afterwards, opening any path with these `s3_opts` restores it.

  The file and its WAL are copied next to it and read from that copy. Its
  schema and rows are copied into a new MVCC database (rowids, AUTOINCREMENT
  sequences, indexes, views, triggers, foreign keys and `user_version`
  included), which is checked with `PRAGMA integrity_check`, uploaded as the
  database's first snapshot under the writer lease, and deleted. **The
  source is never written to**, and nothing in S3 is visible until the
  import completes, so an import that fails (or a process that dies during
  one) can simply be run again.

  Stop every process that writes the file first: changes made during or
  after the import aren't in S3. Opening `path` itself with `s3:` afterwards
  replaces it with the restored copy (the same data).

  ## Options

    * `:verify` - `:checksum` (default): the stored snapshot's size and
      CRC32C must match the new database (every uploaded part also carries
      a SHA-256 the store checks). `:restore`: also download the database
      into a temporary file and compare.
    * `:encryption` - a key ring: the new database is encrypted with its
      active key (open it with a ring holding that key); `false` stores it
      unencrypted. Required, see "Encryption".
    * `:source_encryption` - the key of an encrypted source,
      `[cipher: ..., key: ...]` (a plaintext source is read without it).

  Returns `{:ok, info}` with `:epoch`, `:size` (bytes of the new database
  file), `:stored_size` (bytes in S3, compressed), `:objects` (tables,
  indexes, views and triggers), `:rows` and `:sequences_not_advanced`, or
  `{:error, message}`:

    * the prefix already holds a database (import only into an empty one);
    * the file has WITHOUT ROWID tables or virtual tables (such as fts5),
      which MVCC doesn't support, has a hot rollback journal, can't be read
      (an encrypted file needs `:source_encryption`), or the new database
      fails `PRAGMA integrity_check`;
    * a table has columns named `rowid`, `_rowid_` and `oid` (its rowids
      can't be read);
    * a table of the new database differs from the source's (its rows with
      their rowids are compared), or an AUTOINCREMENT sequence doesn't match
      the source's `sqlite_sequence`: an engine bug, named with its tables;
      nothing is imported;
    * the file is larger than 5 TiB (a snapshot must fit in one S3
      object);
    * there's no room next to the file for the two temporary copies;
    * the file is open with `s3:` in this VM;
    * an S3 error, the lease held by a writer, or an unenforced conditional
      write (as for an open).

  Rows keep their rowids. AUTOINCREMENT sequences carry over, also past
  rows deleted from the top and in emptied tables. `:sequences_not_advanced`
  lists the exceptions: emptied AUTOINCREMENT tables whose CHECK constraints
  refuse a placeholder row (zero, empty text, empty blob in the NOT NULL
  columns). Their ids start again at 1 instead of after the ids the deleted
  rows had, and a warning is logged.
  """
  @spec import(Path.t(), keyword() | map(), keyword()) :: {:ok, map()} | {:error, term()}
  def import(path, s3_opts, opts \\ []) do
    path = to_string(path)
    verify = Keyword.get(opts, :verify, :checksum)

    s3_opts = with_encryption_choice(s3_opts, opts[:encryption])
    source = opts[:source_encryption] |> Keys.normalize() |> Keys.native()

    Telemetry.span([:s3, :import], %{path: path}, fn ->
      case Keys.run(
             Keys.normalize(opts[:encryption]),
             &Native.s3_import(path, s3_opts, verify, &1, source)
           ) do
        {:ok, %{sequences_not_advanced: [_ | _] = tables} = info} ->
          Logger.warning(
            "S3 import of #{path}: the AUTOINCREMENT sequences of #{Enum.join(tables, ", ")} " <>
              "(empty tables) start again at 1; ids of their deleted rows can be handed out again"
          )

          {:ok, info}

        other ->
          other
      end
    end)
  end

  @doc """
  Generates a random encryption key for the `:encryption` option: 32 bytes
  from a cryptographically secure source, as 64 lowercase hex characters.

      encryption: [cipher: "aegis256", key: Sediment.S3.generate_key()]

  Store it like any other secret (an environment variable such as
  `DATABASE_ENCRYPTION_KEY`, a secrets manager): the data can't be read, nor
  restored, without it.
  """
  @spec generate_key() :: String.t()
  def generate_key, do: 32 |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower)

  @doc """
  Moves the database at the S3 location in `s3_opts` to another key of its
  ring: the active one, or `:to`.

  `path` is a local working copy (like an open's path; the database's files
  there are replaced). The rekey is a writer open that re-encrypts what it
  restored, then a clean close, which leaves the copy warm under the new
  key, so the next open of `path` downloads nothing. It:

    1. takes the writer lease; like `destroy/2`, it refuses while the
       database is open in this VM, and any unexpired lease (this owner's
       too) unless `force: true`; it also refuses while `path` is open in
       this VM (without `:s3`, or with another prefix);
    2. takes the database over, resolves its current key from the ring (by
       id and fingerprint), and restores and checkpoints it under that key;
    3. re-encrypts the file page by page under the new key, checks that
       every page's plaintext is unchanged and runs `PRAGMA
       integrity_check` on the result;
    4. uploads it as a full snapshot and publishes it as a new epoch whose
       manifest record names the new key, with `If-Match` on its takeover.

  A writer that took the database over meanwhile makes the last step fail
  with `{:error, {:fenced, %{current_key: id, fingerprint: fp, message:
  message}}}`, naming the key the manifest has now; everything that writer
  committed is kept. A crash or error before the last step leaves the
  database under the old key, working.

  **Only `{:ok, _}` is an outcome.** After any other result (fenced,
  `{:error, {:indeterminate, message}}`, a store error) the switch may have
  happened, or may still happen: a manifest write in flight can land after
  the error is reported, and a `key_census/1` taken in between shows the
  old key. The only way to know is to run `rekey/3` again (`force: true`,
  or after the lease's TTL, when a crashed run still holds the lease): its
  takeover ends any write still in flight, and it returns `{:ok, :current}`
  or completes the rekey. Until a run returns `{:ok, _}`, keep both keys in
  every ring.

  The epoch it ends and every epoch retained then are kept, whatever
  `retain_epochs` is, until `release_key/4`: point-in-time restore still
  reaches the time before the rekey, with the old key. The old key is
  recorded as retired: a later rekey to it (by id or by key) is refused
  unless `allow_reuse: true`, and even then only once its epochs were
  released.

  Every node that opens the database needs the new key in its ring before
  the rekey (or a `key_for` that returns it): an open whose ring lacks it
  is refused.

  **Cost.** The database is unavailable to writers for the whole rekey:
  the download (none when `path` holds a warm copy), the fold, the
  re-encryption pass, `integrity_check`, the verification pass and the
  upload of a full snapshot, all proportional to its size. Opens meanwhile
  get `"s3 lease held ..."` and retry; the rekey renews its lease
  throughout. It needs about twice the database's size free next to `path`
  (the copy beside it; it refuses to start without), and about 32 bytes of
  memory per page plus what `integrity_check` takes.

  ## Options

    * `:encryption` - the key ring (required): it must hold the current key
      and the new one (or resolve them with `key_for`).
    * `:to` - the id of the key to move to (default: the ring's active key).
      Both keys must use the same cipher.
    * `:force` - take the lease even while a writer holds it. That writer
      is fenced: it acknowledges no further commit, and with `durability:
      :async` the commits it acknowledged but had not uploaded yet are lost,
      as with `destroy/2`. Use it for a writer that can't be stopped.
    * `:allow_reuse` - allow a retired key (see above).

  Returns `{:ok, %{from: id, to: id, epoch: epoch}}`, `{:ok, :current}` when
  the database is under that key already (nothing is written),
  `{:error, {:fenced, map}}`, `{:error, {:indeterminate, message}}` when the
  publication's answer was lost and the manifest couldn't be read back, or
  `{:error, message}`. See above: run it again after any error.
  """
  @spec rekey(Path.t(), keyword() | map(), keyword()) ::
          {:ok, %{from: String.t(), to: String.t(), epoch: String.t()} | :current}
          | {:error, term()}
  def rekey(path, s3_opts, opts \\ []) do
    path = to_string(path)
    to = opts[:to] && to_string(opts[:to])
    force = boolean!(opts, :force, "rekey/3")
    allow_reuse = boolean!(opts, :allow_reuse, "rekey/3")

    Telemetry.span([:s3, :rekey], %{path: path}, fn ->
      opts[:encryption]
      |> required_ring("rekey/3")
      |> Keys.run(&Native.s3_rekey(path, s3_opts, &1, to, force, allow_reuse))
    end)
  end

  @doc """
  Drops the retained epochs encrypted under `key_id` from the history of
  the database at the S3 location in `s3_opts`, and deletes their objects:
  point-in-time restore no longer reaches the time they cover. Afterwards
  `key_census/1` no longer lists the key, and it can leave the ring.

  It refuses the current key (rekey first). It is a writer open with a
  clean close at `path`, with the same lease rules as `rekey/3` (`:force`),
  and needs the `:encryption` ring holding the current key.

  This removes what the manifest references. A bucket with versioning
  keeps older object versions, and other hosts may keep local working
  copies under the old key: after a key leaked, purge those too.

  Returns `{:ok, %{key: id, epochs: [epoch]}}` (no epochs, and nothing
  written, when none was under the key) or `{:error, reason}`, with the same
  forms as `rekey/3`'s. As with `rekey/3`, only `{:ok, _}` is an outcome:
  after an error the release may still happen, so run it again; keep the
  key in the rings until a run returns `{:ok, _}` and the census no longer
  lists it.
  """
  @spec release_key(Path.t(), keyword() | map(), String.t(), keyword()) ::
          {:ok, %{key: String.t(), epochs: [String.t()]}} | {:error, term()}
  def release_key(path, s3_opts, key_id, opts \\ []) do
    path = to_string(path)
    force = boolean!(opts, :force, "release_key/4")

    Telemetry.span([:s3, :release_key], %{path: path}, fn ->
      opts[:encryption]
      |> required_ring("release_key/4")
      |> Keys.run(&Native.s3_release_key(path, s3_opts, &1, to_string(key_id), force))
    end)
  end

  # Checked here: a value the NIF can't decode raises badarg with every
  # argument (the key ring, S3 credentials) in the stack trace.
  defp boolean!(opts, name, call) do
    case Keyword.get(opts, name, false) do
      value when is_boolean(value) -> value
      _ -> raise ArgumentError, "Sediment.S3.#{call}: #{inspect(name)} must be true or false"
    end
  end

  defp required_ring(encryption, call) do
    case Keys.normalize(encryption) do
      ring when is_map(ring) ->
        ring

      _ ->
        raise ArgumentError, "Sediment.S3.#{call} needs the :encryption key ring"
    end
  end

  @doc """
  Which keys the database at the S3 location in `s3_opts` depends on: the
  current epoch's key, and the retained epochs (point-in-time restore) by
  the key they are encrypted with. A key can leave the ring once it is
  neither current nor retained. It reads `manifest.json` only: no lease,
  no writes.

  Returns `{:ok, %{current: id | nil, retained: %{id => [epoch]}}}`
  (`current: nil` for an unencrypted database), `{:ok, :no_database}`
  when the location holds none (also after `destroy/2`), or
  `{:error, reason}`.

  It is an observation, not a resolution: a rekey or release that returned
  an error may still switch the manifest after the census read it. Only a
  re-run of the operation resolves that.
  """
  @spec key_census(keyword() | map()) ::
          {:ok, %{current: String.t() | nil, retained: %{String.t() => [String.t()]}}}
          | {:ok, :no_database}
          | {:error, term()}
  def key_census(s3_opts), do: Keys.scrubbed(fn -> Native.s3_key_census(s3_opts) end)

  @doc false
  # `encryption: false` (the explicit opt-out of encryption) travels with
  # the S3 options to the native code, which refuses S3 databases that have
  # neither a key nor the opt-out.
  @spec with_encryption_choice(keyword() | map() | nil, keyword() | false | nil) ::
          keyword() | map() | nil
  def with_encryption_choice(s3, false) when is_list(s3), do: Keyword.put(s3, :encryption, false)
  def with_encryption_choice(%{} = s3, false), do: Map.put(s3, :encryption, false)
  def with_encryption_choice(s3, _encryption), do: s3

  # Runs an S3 operation on one connection of a DBConnection pool; see
  # Sediment.Connection.handle_execute/4.
  defp pool_call(conn, op, opts \\ []) do
    check_pool!(conn, op)

    query = %Sediment.Query{
      statement: "-- sediment s3 #{inspect(op)}",
      command: {:sediment_s3, op}
    }

    case DBConnection.execute(conn, query, [], opts) do
      {:ok, _query, %Sediment.Result{rows: [[result]]}} -> result
      {:error, error} -> {:error, error}
    end
  end

  # DBConnection.execute/4 on a process that isn't a pool (an Ecto repo's
  # supervisor, say) only fails after the checkout timeout: refuse at once.
  defp check_pool!(%DBConnection{}, _op), do: :ok

  defp check_pool!(conn, op) do
    case GenServer.whereis(conn) do
      pid when is_pid(pid) and node(pid) == node() -> check_pool_pid!(pid, conn, op)
      # Another node's process: DBConnection reaches it as usual.
      pid when is_pid(pid) -> :ok
      {_name, _node} -> :ok
      nil -> raise ArgumentError, "#{call(op)}: no process #{inspect(conn)}"
    end
  end

  defp check_pool_pid!(pid, conn, op) do
    case Process.alive?(pid) && :proc_lib.translate_initial_call(pid) do
      {module, _fun, _arity}
      when module in [DBConnection.ConnectionPool, DBConnection.Ownership.Manager] ->
        :ok

      {:supervisor, Ecto.Repo.Supervisor, _arity} ->
        raise ArgumentError,
              "#{call(op)} expects a db reference or a DBConnection pool, got the Ecto repo " <>
                "#{inspect(conn)}: use the adapter's s3_* functions (Ecto.Adapters.Sediment), " <>
                "or pass Ecto.Adapter.lookup_meta(repo).pid"

      _ ->
        raise ArgumentError,
              "#{call(op)} expects a db reference or a DBConnection pool, got #{inspect(conn)}"
    end
  end

  defp call({op, _timeout}), do: call(op)
  defp call(op), do: "Sediment.S3.#{op}"

  @doc """
  Waits until everything committed so far is durable in S3, at most
  `timeout` milliseconds.

  With `durability: :async` a commit returns once it is in the local log,
  and a background uploader makes it durable in S3 shortly after; `flush/2`
  waits for that. Returns `:ok`, or `{:error, reason}` when the
  timeout expires or the writer is fenced (the reason says up to which log
  offset the data is durable). With `durability: :sync` every commit is
  already durable, and `flush/2` returns at once.

  With a `DBConnection` pool, one connection of the pool waits; the storage
  (and so what is flushed) is shared by all connections to the database.
  See also the `sync: true` option of `Sediment.transaction/3` and
  `Sediment.query/4`.
  """
  @spec flush(Sediment.Engine.db() | DBConnection.conn(), timeout()) :: :ok | {:error, term()}
  def flush(db, timeout \\ 15_000)

  def flush(db, timeout) when is_reference(db) do
    Telemetry.span([:s3, :flush], %{timeout: timeout}, fn ->
      case Native.s3_flush(db, timeout_ms(timeout), Native.admit(db)) do
        {:ok, _durable} -> :ok
        {:error, _reason} = error -> error
      end
    end)
  end

  # The pool checkout must outlast the wait.
  def flush(conn, :infinity), do: pool_call(conn, {:flush, :infinity}, timeout: :infinity)
  def flush(conn, timeout), do: pool_call(conn, {:flush, timeout}, timeout: timeout + 5_000)

  @doc """
  Acknowledges that commits were lost, so that `flush/2` and `sync: true`
  succeed again. Returns what was lost (the `lost` field of `info/1`), or
  `nil`.

  With `durability: :async`, commits still queued when a writer is fenced
  (another writer took the lease, or S3 failed past the retries) or closes
  without uploading them are lost; a pool reconnects and restores what was
  durable. From then on `flush/2` and `sync: true` fail with an error naming
  the lost range, on the reconnected pool too, so the loss can't go
  unnoticed. Once the application has dealt with it, `acknowledge_loss/1`
  clears it for the database.
  """
  @spec acknowledge_loss(Sediment.Engine.db() | DBConnection.conn()) ::
          {:ok, loss() | nil} | {:error, term()}
  def acknowledge_loss(db) when is_reference(db), do: Native.s3_acknowledge_loss(db)
  def acknowledge_loss(conn), do: pool_call(conn, :acknowledge_loss)

  @doc """
  The S3 requests this OS process has sent so far (every database, retries
  and multipart parts included), by operation, with the price class
  providers bill it in: `"A"` (writes and lists), `"B"` (reads) or
  `"free"` (deletes).

  With the environment variable `SEDIMENT_S3_METER_DIR` set, each request is
  also appended to `requests-<os pid>.log` in that directory before it is
  sent, and no request is sent while a file named `STOP` exists there (a
  budget guard for runs against a paid provider).
  """
  @spec request_counts() :: %{String.t() => %{class: String.t(), count: non_neg_integer()}}
  def request_counts do
    Map.new(Native.s3_request_counts(), fn {op, class, n} -> {op, %{class: class, count: n}} end)
  end

  @doc false
  # For `sync: true`: waits until what the connection's last autocommit
  # statement or transaction committed is durable, and returns at once when
  # it committed nothing (so reads never wait for S3). Emits the same
  # [:s3, :flush] span as flush/2.
  @spec flush_commit(Sediment.Engine.db(), timeout()) :: :ok | {:error, term()}
  def flush_commit(db, timeout) do
    Telemetry.span([:s3, :flush], %{timeout: timeout}, fn ->
      case Native.s3_flush_commit(db, timeout_ms(timeout), Native.admit(db)) do
        {:ok, _} -> :ok
        {:error, _reason} = error -> error
      end
    end)
  end

  # :infinity (a DBConnection timeout, say) as a wait the native side takes
  # in milliseconds: a day, in practice until cancel/1 or a disconnect.
  defp timeout_ms(:infinity), do: 86_400_000
  defp timeout_ms(ms) when is_integer(ms) and ms >= 0, do: ms

  @doc """
  The `:shutdown` a `DBConnection` pool of this S3 database needs: closing a
  connection with `durability: :async` waits for pending uploads (up to
  `:close_timeout_ms`, 10 s by default, twice: the connection, then the
  storage), and the pool must not kill it meanwhile, or the database would
  still be closing after the pool stopped. `Sediment.start_link/1` and
  `Sediment.child_spec/1` use it by default; pass it as `:shutdown` when
  starting `DBConnection` (or an Ecto repo) yourself.
  """
  @spec shutdown_timeout(keyword() | map() | nil) :: non_neg_integer()
  def shutdown_timeout(s3_opts) do
    close_ms =
      case close_timeout_ms(s3_opts) do
        ms when is_integer(ms) -> ms
        _ -> 10_000
      end

    2 * close_ms + 5_000
  end

  defp close_timeout_ms(opts) when is_list(opts), do: Keyword.get(opts, :close_timeout_ms)

  defp close_timeout_ms(opts) when is_map(opts),
    do: Map.get(opts, :close_timeout_ms, Map.get(opts, "close_timeout_ms"))

  defp close_timeout_ms(_opts), do: nil

  @doc false
  # Pool-wide refresh requests, per database path.
  @spec generation(String.t()) :: non_neg_integer()
  def generation(path), do: path |> generation_key() |> generation_at()

  @doc false
  # The key for generation_at/1, computed once per connection: expanding the
  # path on every statement showed up in profiles.
  @spec generation_key(String.t()) :: term()
  def generation_key(path), do: {__MODULE__, :generation, Path.expand(to_string(path))}

  @doc false
  @spec generation_at(term()) :: non_neg_integer()
  def generation_at(key), do: :persistent_term.get(key, 0)

  @doc false
  @spec bump_generation(String.t()) :: pos_integer()
  def bump_generation(path) do
    next = generation(path) + 1
    :persistent_term.put(generation_key(path), next)
    next
  end
end
