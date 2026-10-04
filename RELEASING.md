# Releasing Sediment

Releases are published to Hex by `.github/workflows/release.yml` when a GitHub
release is published. Nobody publishes from a laptop.

The Hex package ships the NIF's sources and a checksum file; the precompiled
NIFs themselves are assets of the GitHub release, where `Sediment.Native`
downloads them from (`.../releases/download/v<version>`).

## Steps

1. On a branch, set the version in `mix.exs` (`@version`) and turn the
   CHANGELOG's `## x.y.z (unreleased)` heading into `## x.y.z (YYYY-MM-DD)`.
   Merge it to `main` through a pull request with green CI.
2. Check the package locally. `mix hex.build` refuses to build without
   `checksum-Elixir.Sediment.Native.exs` (a package without it can't load
   the NIF); the release workflow generates it. To look at the file list,
   create a placeholder and remove it afterwards:
   `echo '%{}' > checksum-Elixir.Sediment.Native.exs && mix hex.build --unpack`.
3. Optionally run the `NIF` workflow by hand (Actions, "Run workflow") on
   that commit: it builds every target as workflow artifacts and nothing
   else, so a target that no longer builds shows up before the release.
4. Create a GitHub release with the tag `vx.y.z` on that `main` commit, with
   the CHANGELOG entry as its notes, and publish it.
5. The release workflow:
   1. checks that the tag matches `@version`, that the CHANGELOG has a dated
      entry for it and that the commit is on `main`;
   2. runs the full CI from a clean build (no caches) and, in parallel,
      builds the NIF for every target (`.github/workflows/nif.yml`, no
      caches);
   3. when all of that passed, attaches the NIF tarballs to the release.
      This is the only job with write access, and it checks nothing out;
   4. waits for approval of the `hex` environment. Approve it, and it
      downloads the NIFs from the release, writes the checksum file with
      `mix rustler_precompiled.download Sediment.Native --all`, checks that
      it lists exactly the NIFs built in step 2 with the same SHA-256, and
      runs `mix hex.publish`.

   If any target fails to build, nothing is uploaded or published. Fix it,
   delete the release and its tag, and release again. If a run fails after
   the upload, delete the release's NIF assets before re-running it: the
   upload never replaces existing assets.
6. Release ecto_sediment afterwards if it needs the new version: its Hex
   dependency is `{:sediment, "~> 0.1.0"}`, and its release workflow checks
   that the required sediment version exists on Hex.

### Targets

Linux gnu and musl (x86_64, aarch64), macOS (aarch64, x86_64) and Windows
(x86_64 msvc), all NIF version 2.15, which loads on OTP 22 and later. The
gnu builds run on Ubuntu 22.04 (glibc 2.35); the musl builds run in a
pinned `rust:alpine` image on the native runner. Adding a target means a
matrix entry in `nif.yml` and the list in `lib/sediment/native.ex`, which
must match.

A broken release is retired with `mix hex.retire sediment x.y.z <reason>`;
publish a fixed patch release instead of replacing it.

## One-time repository settings

These live in the GitHub settings, not in files. Check them when the
repository is created, and again after changing who maintains it.

- [ ] Environment `hex`: secret `HEX_API_KEY`; required reviewer: the
      maintainer; deployment branches and tags: tags matching `v*` only;
      administrators can't bypass the protection rules.
- [ ] No repository- or organization-level secret holds a Hex key.
- [ ] Branch protection on `main`: pull request required, CI required (all
      `CI` jobs), no force pushes, no deletions, applies to administrators.
- [ ] Actions: "Require approval for all outside collaborators" for fork pull
      request workflows.
- [ ] Actions: "Allow GitHub Actions to create and approve pull requests"
      disabled; default workflow permissions read-only.
- [ ] Secret scanning and push protection enabled.
- [ ] Dependabot alerts and security updates enabled (version updates come
      from `.github/dependabot.yml`).

## Hex API key

- Create it with `mix hex.user key generate --key-name sediment-release-YYYY
  --permission api:write` (or on hex.pm, with only the publish permission for
  this package) and store it only as the `hex` environment's `HEX_API_KEY`.
- Rotate it yearly, and at once if it may have leaked or a maintainer
  leaves: generate a new key, replace the environment secret, then revoke the
  old one with `mix hex.user key revoke sediment-release-<old year>` (or on
  hex.pm).
- Never put the key in a repository secret, a workflow file or a local
  shell history.
