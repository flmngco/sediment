# Releasing Sediment

Releases are published to Hex by `.github/workflows/release.yml` when a GitHub
release is published. Nobody publishes from a laptop.

## Steps

1. On a branch, set the version in `mix.exs` (`@version`) and turn the
   CHANGELOG's `## x.y.z (unreleased)` heading into `## x.y.z (YYYY-MM-DD)`.
   Merge it to `main` through a pull request with green CI.
2. Check the package locally: `mix hex.build` and look at the file list.
3. Create a GitHub release with the tag `vx.y.z` on that `main` commit, with
   the CHANGELOG entry as its notes, and publish it.
4. The release workflow runs the full CI from a clean build (no caches),
   checks that the tag matches `@version`, that the CHANGELOG has a dated
   entry for it and that the commit is on `main`, and then waits for approval
   of the `hex` environment. Approve it, and it runs `mix hex.publish`.
5. Release ecto_sediment afterwards if it needs the new version: its Hex
   dependency is `{:sediment, "~> 0.1.0"}`, and its release workflow checks
   that the required sediment version exists on Hex.

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
