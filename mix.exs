defmodule Sediment.MixProject do
  use Mix.Project

  @version "0.1.0-beta.2"
  @source_url "https://github.com/flmngco/sediment"

  def project do
    [
      app: :sediment,
      version: @version,
      source_url: @source_url,
      homepage_url: @source_url,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      package: package(),
      name: "Sediment",
      description:
        "Elixir driver for the Turso database engine (SQLite-compatible): exqlite-compatible API, MVCC, encryption and S3-backed durability. Not affiliated with Turso.",
      docs: docs()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :crypto]
    ]
  end

  def cli do
    [
      preferred_envs: [ci: :test]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:db_connection, "~> 2.1"},
      {:telemetry, "~> 0.4 or ~> 1.0"},
      {:rustler_precompiled, "~> 0.10"},
      {:rustler, "~> 0.38", runtime: false},
      {:table, "~> 0.1.0", optional: true},
      {:temp, "~> 0.4", only: [:dev, :test]},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:ex_slop, "~> 0.4", only: [:dev, :test], runtime: false},
      {:reach, "~> 2.0", only: [:dev, :test], runtime: false},
      {:ex_dna, "~> 1.0", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.0", only: [:dev, :test], runtime: false},
      {:vibe_kit, "~> 0.1", only: :dev, runtime: false}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      files: ~w(
        lib
        native/sediment_nif/src
        native/sediment_nif/Cargo.toml
        native/crc-fast-shim/src
        native/crc-fast-shim/Cargo.toml
        .cargo/config.toml
        Cargo.toml
        Cargo.lock
        checksum-Elixir.Sediment.Native.exs
        guides
        docs
        .formatter.exs
        mix.exs
        README.md
        CHANGELOG.md
        LICENSE
      ),
      # The NIF's Rust tests (and their fixtures) are compiled only by cargo test.
      exclude_patterns: [~r"native/sediment_nif/src/s3/tests/"],
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url,
        "Ecto adapter" => "https://github.com/flmngco/ecto_sediment",
        "Turso" => "https://github.com/tursodatabase/turso"
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      extras: [
        "README.md",
        "guides/getting_started.md",
        "guides/turso_extensions.md",
        "guides/s3.md",
        "guides/s3_providers.md",
        "guides/exqlite_parity.md",
        "guides/leaving_sediment.md",
        "CHANGELOG.md",
        "bench/RESULTS.md",
        "docs/s3-durability.md",
        {"formal/README.md", filename: "formal-model", title: "Formal model (TLA+)"},
        "docs/upstream/turso-durable-storage-group-commit.md",
        "docs/upstream/turso-mvcc-hot-row-versions.md",
        "docs/upstream/turso-sequence-inner-tx-busy.md"
      ],
      groups_for_extras: [
        Guides: ~r"guides/",
        Design: ~r"docs/|formal/"
      ],
      groups_for_modules: [
        "Turso extensions": [Sediment.CDC, Sediment.Vector, Sediment.S3],
        Telemetry: [Sediment.Telemetry]
      ]
    ]
  end

  defp aliases() do
    [
      ci: [
        "compile --warnings-as-errors",
        "format --check-formatted",
        "test",
        "credo --strict",
        "ex_dna --max-clones 0",
        "reach.check --arch --smells",
        "cmd env MIX_ENV=dev mix docs --warnings-as-errors",
        # the Rust crate too, as in .github/workflows/ci.yml (cargo test runs there as well)
        "cmd cargo fmt --all --check --manifest-path native/sediment_nif/Cargo.toml",
        "cmd cargo clippy --workspace --all-targets -- -D warnings"
      ]
    ]
  end
end
