# S3 providers

S3 durability needs more from an object store than plain GET and PUT. The
writer fences stale writers with conditional writes:

* `If-None-Match: *` on PUT (create only if absent). Each log frame is
  written this way, so two writers can never both store a frame at the same
  log offset.
* `If-Match: <etag>` on PUT (compare-and-swap). The manifest and the lease
  change only this way, so a stale writer's late manifest update is refused.

Every upload also carries `x-amz-checksum-sha256` of its body (each
multipart part too). A server must reject a body that doesn't match: without
it, SeaweedFS stores an empty object when a PUT's connection breaks right
after its headers, and an empty manifest, lease or log object would make the
database unopenable. The probe's conditional PUTs carry the checksum as
well, so a provider that refuses checksummed uploads fails at open.

A provider that silently ignores these headers would accept both writes,
and the second would overwrite the first. That would lose acknowledged
commits or let two writers diverge. So every writer checks the provider when it
opens (see "The probe" below) and refuses to run if the headers aren't
enforced.

## Compatibility

| Provider | `If-None-Match: *` on PUT | `If-Match` on PUT | Status |
| --- | --- | --- | --- |
| AWS S3 | yes (since August 2024) | yes (since November 2024) | Supported. |
| MinIO | yes | yes | Supported. The full suite passed against it once. |
| SeaweedFS | yes | yes | Supported. Tested here (full suite). |
| Cloudflare R2 | yes | yes | Should work; not tested here. |
| Tigris | yes | yes | Supported. Tested here (S3 suite, 40 minutes of crash torture, benchmarks; see the S3 guide). |
| Backblaze B2 (S3 API) | not documented | not documented | Unknown; the probe decides. |
| Google Cloud Storage (XML/S3 interop API) | no (GCS uses `x-goog-if-generation-match`) | no | Not supported through the S3 API. |

"Tested here" means `mix test --only s3` and the cargo S3 tests pass against
it (see "Running the tests against another provider"). The other rows
reflect provider documentation at the time of writing. Providers change,
so rely on the probe rather than on this table.

Other requirements, which every provider above meets:

* strong read-after-write consistency for PUT, GET and LIST;
* ETags returned on PUT and GET;
* `last_modified` in listings (point-in-time restore uses it);
* multipart upload for snapshots larger than one part: every part but the
  last has the same size (8 MiB, larger for databases above about 70 GiB),
  as R2 requires, and at most 10,000 parts.

## The probe

When a writer opens (`mode: :writer`, the default), it runs these requests
against a key under `<prefix>/probe/` before it touches the database:

1. create-only PUT: must succeed;
2. create-only PUT of the same key again: must be refused (412);
3. PUT with `If-Match` on a made-up ETag: must be refused (412);
4. PUT with `If-Match` on the real ETag: must succeed;

and then deletes the key. If step 2 or 3 succeeds, the open fails with:

```
s3 config: this S3 provider does not enforce If-None-Match: * (create-only PUT),
which S3 durability needs to fence writers; refusing to open (see guides/s3_providers.md)
```

(or the same for `If-Match`). The probe runs once per endpoint, region and
bucket per VM, which costs 4 PUTs and 1 DELETE. Replicas and
`Sediment.S3.restore/3` never write, so they don't probe.

`verify_conditional_writes: false` skips the probe. Only do that for a
provider you have verified yourself, since without enforced conditional writes
a second writer or a writer's late request can corrupt the database.

## Bucket lifecycle: incomplete multipart uploads

Snapshots larger than one part (8 MiB, or more for very large databases) go up as multipart uploads. A failed
upload is aborted, but a writer that crashes or is killed during one leaves its
uploaded parts behind. They're invisible in listings and stored (and billed)
until the upload is aborted. Add a lifecycle rule that aborts incomplete
multipart uploads after a few days, if the provider supports one. On AWS S3:

```json
{"Rules": [{"ID": "abort-incomplete-multipart", "Status": "Enabled",
            "Filter": {"Prefix": ""},
            "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}}]}
```

The days must exceed the longest snapshot upload. Don't add rules that expire
or transition objects under a database's prefix: the manifest references its
snapshots and log objects for as long as they're needed, and garbage collection
deletes them when they aren't.

## Provider notes

**AWS S3.** No configuration beyond `bucket`, `region` and credentials
(or the standard `AWS_*` environment variables and instance roles). Directory
buckets (S3 Express One Zone) are untested.

**MinIO.**

```elixir
s3: [bucket: "db", endpoint: "http://minio:9000", access_key_id: "...", secret_access_key: "..."],
encryption: [cipher: "aegis256", key: key]  # or encryption: false
```

`:endpoint` implies path-style URLs. MinIO requires signed requests, so
create the bucket beforehand (for example `mc mb local/db`).

**Cloudflare R2.** Use `endpoint: "https://<account id>.r2.cloudflarestorage.com"`
and `region: "auto"`.

**Tigris.** Use `endpoint: "https://t3.storage.dev"` and `region: "auto"`
(tested; path-style requests and checksummed conditional PUTs work).

**SeaweedFS.** `endpoint: "http://host:8333"`. Every bucket is a separate
SeaweedFS collection that reserves volume slots, so prefer one bucket with
many prefixes.

## Running the tests against another provider

The S3 test suites use SeaweedFS on `127.0.0.1:8333` by default. To use
another server:

```sh
export S3_TEST_ENDPOINT=http://127.0.0.1:9000
export S3_TEST_ACCESS_KEY_ID=...
export S3_TEST_SECRET_ACCESS_KEY=...
export S3_TEST_BUCKET=sediment-tests   # must exist unless the server allows unsigned bucket creation
mix test --only s3
(cd native/sediment_nif && cargo test s3::)
```
