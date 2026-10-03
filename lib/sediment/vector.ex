defmodule Sediment.Vector do
  @moduledoc """
  Encodes and decodes turso vector blobs.

  Turso has built-in vector functions (`vector32/1`, `vector64/1`,
  `vector_distance_cos/2`, `vector_distance_l2/2`, `vector_extract/1`, ...).
  Vectors can be written as text (`vector32('[1, 2, 3]')`) or bound directly
  as blobs built with `new/2`:

      embedding = Sediment.Vector.new([0.1, 0.2, 0.3])
      Sediment.query(conn, "INSERT INTO docs (embedding) VALUES (?)", [embedding])

      Sediment.query(conn,
        "SELECT id FROM docs ORDER BY vector_distance_cos(embedding, ?) LIMIT 5",
        [Sediment.Vector.new(query_embedding)])

  Only dense `:f32` and `:f64` vectors are supported here; use the SQL
  functions for the quantized types (`vector8/1`, `vector1bit/1`).
  """

  @f64_tag 2

  @type type :: :f32 | :f64

  @doc """
  Builds a vector blob from a list of numbers, tagged `{:blob, binary}` so it
  binds as a blob.

      iex> Sediment.Vector.new([1, 2])
      {:blob, <<0, 0, 128, 63, 0, 0, 0, 64>>}

  """
  @spec new([number()], type()) :: {:blob, binary()}
  def new(values, type \\ :f32) when is_list(values) do
    {:blob, encode(values, type)}
  end

  defp encode(values, :f32), do: for(v <- values, into: <<>>, do: <<v::float-32-little>>)

  defp encode(values, :f64),
    do: <<for(v <- values, into: <<>>, do: <<v::float-64-little>>)::binary, @f64_tag>>

  @doc """
  Decodes a vector blob as returned by turso into a list of floats.

      iex> Sediment.Vector.to_list(<<0, 0, 128, 63, 0, 0, 0, 64>>)
      [1.0, 2.0]

  """
  @spec to_list(binary()) :: [float()]
  def to_list(blob) when rem(byte_size(blob), 4) == 0,
    do: for(<<v::float-32-little <- blob>>, do: v)

  def to_list(blob)
      when rem(byte_size(blob), 8) == 1 and
             binary_part(blob, byte_size(blob) - 1, 1) == <<@f64_tag>> do
    size = byte_size(blob) - 1
    <<data::binary-size(^size), @f64_tag>> = blob
    for <<v::float-64-little <- data>>, do: v
  end

  def to_list(_blob), do: raise(ArgumentError, "not a dense f32 or f64 vector blob")
end
