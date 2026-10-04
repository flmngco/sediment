defmodule SedimentBench.MixProject do
  use Mix.Project

  # A separate project so exqlite never becomes a dependency of sediment.
  def project do
    [app: :sediment_bench, version: "0.1.0", elixir: "~> 1.17", deps: deps()]
  end

  def application, do: [extra_applications: [:logger]]

  defp deps do
    [
      {:sediment, path: ".."},
      {:exqlite, "~> 0.41"},
      {:benchee, "~> 1.3"}
    ]
  end
end
