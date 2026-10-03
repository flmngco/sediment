# Contributing to Sediment

Thanks for helping. This page covers the development setup, the checks a
change has to pass and what we expect from a pull request.

## Setup

You need Elixir 1.18 or newer (OTP 27+), a stable Rust toolchain (the NIF is
compiled from source with Rustler) and Docker for the S3 test server.

Sediment and its Ecto adapter are developed side by side:

```sh
git clone https://github.com/flmngco/sediment
git clone https://github.com/flmngco/ecto_sediment   # optional: the adapter
cd sediment
mix deps.get
mix test
```

`ecto_sediment` depends on `../sediment` by default; `SEDIMENT_PATH` points
it somewhere else.

### S3 test server

The S3 suites run against SeaweedFS 4.48 on `127.0.0.1:8333` (the same image
and version CI uses):

```sh
docker run -d --name sediment-seaweedfs -p 127.0.0.1:8333:8333 chrislusf/seaweedfs:4.48
```

The tests create the bucket (`sediment-tests`) themselves. `S3_TEST_ENDPOINT`,
`S3_TEST_BUCKET` and `S3_TEST_REGION` point them at another server. MinIO
works too; see [the providers guide](guides/s3_providers.md#running-the-tests-against-another-provider)
for the command and the variables.

## Checks

`mix ci` is what a change has to pass. It runs, in the test environment:
compile with warnings as errors, `mix format --check-formatted`, `mix test`,
`credo --strict`, `ex_dna`, `reach.check`, the docs build with warnings as
errors, and `cargo fmt --check` and `cargo clippy -D warnings` for the NIF.

The Rust side on its own:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --release   # includes S3 tests against SeaweedFS
```

### Test suites

`mix test` runs the unit and integration suites. The tagged suites are
excluded by default:

| Command | What it runs | Needs |
|---|---|---|
| `mix test --only s3` | S3 durability, replicas, restore, import/export, fault injection | the S3 test server |
| `mix test --only slow_test` | cancellation, stress, timeouts, fuzz and model tests | a few minutes |
| `mix test --only torture` | S3 crash torture (`TORTURE_MINUTES`, default 30) | the S3 test server |
| `mix test --only soak` | a mixed workload checking memory, NIF resources and scheduler latency (`SOAK_SECONDS`, default 300) | a few minutes |

CI runs `mix ci`, `--only s3`, `--only slow_test` and the cargo commands on
the oldest and newest supported Elixir/OTP versions. Run the torture and soak
suites locally when you change S3 durability or resource handling.

`test/sediment/parity_test.exs` also checks that every public exqlite function
is listed in `guides/exqlite_parity.md`. That part needs an exqlite checkout:
`EXQLITE_SRC=/path/to/exqlite mix test test/sediment/parity_test.exs`.
Without `EXQLITE_SRC` it is skipped.

### The formal model

The S3 protocol has a TLA+ model in `formal/` (see
[formal/README.md](formal/README.md)). Changes to the protocol (leases,
fencing, manifests, garbage collection) should keep it in step:

```sh
formal/bin/check   # every configuration against its expected outcome
```

It needs a JDK and `tla2tools.jar` plus `CommunityModules-deps.jar` in
`~/tools` (or `$TLA_TOOLS`).

## Pull requests

- Keep a pull request to one change, with tests that fail without it.
- `mix ci` must pass. For S3 changes run `--only s3` too, and the Rust tests.
- Add user-visible changes to `CHANGELOG.md` and the guides.
- Workarounds for turso_core bugs name the engine version (0.8.1) and say
  what they guard, so they can be removed when upstream fixes them.
- The S3 object layout and the manifest and lease formats are persistent:
  changes to them need a migration story.

By contributing you agree that your contributions are licensed under the MIT
license of this project.
