defmodule Sediment.S3.Keys do
  @moduledoc false
  # The `:encryption` option as a key ring, and the `key_for` loop.
  #
  # Forms (see "Encryption" and "Rotating keys" in the S3 guide):
  #
  #   * `[cipher: c, key: hex]`: a ring of one key, id "default";
  #   * `[keys: [{"r2", hex}, {"r1", hex, cipher: c}], cipher: c]`: the first
  #     key is active; the cipher defaults to "aegis256";
  #   * `[key_for: fun | {m, f, a}, active: "r2", cipher: c]`: keys resolved
  #     on demand (with `keys:` too, as a cache);
  #   * `false`: no encryption; nil: none given.
  #
  # The native code validates ids and keys (and never puts a key in an
  # error). When it needs a key a `key_for` ring lacks, it answers
  # `{:error, {:key_needed, id}}` before writing anything to S3; `run/2`
  # then asks `key_for` and tries again.

  @default_cipher "aegis256"
  @attempts 3

  @type ring :: %{
          keys: [{String.t(), String.t(), String.t()}],
          active: String.t() | nil,
          cipher: String.t(),
          key_for: (String.t() -> term()) | {module(), atom(), list()} | nil
        }

  defmodule Hidden do
    @moduledoc false
    # The `:encryption` option out of sight, in a closure: a pool's options
    # and a connection's state hold this, and neither inspect (a crash or
    # termination report, `:sys.get_status/1`, an inspected struct) nor
    # Erlang's `~p` prints what a closure captured.
    @enforce_keys [:get]
    defstruct [:get]

    @type t :: %__MODULE__{get: (-> term())}

    defimpl Inspect do
      def inspect(_hidden, _opts), do: "#Sediment.S3.Keys.Hidden<redacted>"
    end
  end

  @doc false
  # The option as kept in a process: every function taking `:encryption`
  # takes it hidden too.
  @spec hide(term()) :: Hidden.t() | false | nil
  def hide(encryption) when encryption in [nil, false], do: encryption
  def hide(%Hidden{} = hidden), do: hidden
  def hide(encryption), do: %Hidden{get: fn -> encryption end}

  @doc false
  @spec normalize(keyword() | map() | Hidden.t() | false | nil) :: ring() | false | nil
  def normalize(encryption), do: sealed(fn -> do_normalize(encryption) end)

  defp do_normalize(nil), do: nil
  defp do_normalize(false), do: false
  defp do_normalize(%Hidden{get: get}), do: do_normalize(get.())
  defp do_normalize(%{} = opts), do: opts |> Map.to_list() |> do_normalize()

  # Every value is checked by type here (the native code checks ids and
  # keys), with fixed messages: a message never shows a value, which may be
  # a key given in the wrong place.
  defp do_normalize(opts) when is_list(opts) do
    cipher = cipher!(Keyword.get(opts, :cipher, @default_cipher), "the cipher")

    cond do
      Keyword.has_key?(opts, :keys) or Keyword.has_key?(opts, :key_for) ->
        %{
          keys: entries(Keyword.get(opts, :keys, []), cipher),
          active: opts[:active] && id!(opts[:active], ":active"),
          cipher: cipher,
          key_for: key_for(opts[:key_for])
        }

      Keyword.has_key?(opts, :key) ->
        %{
          keys: [{"default", cipher, key!(opts[:key], ":key")}],
          active: nil,
          cipher: cipher,
          key_for: nil
        }

      true ->
        raise ArgumentError,
              "encryption: expected [cipher: ..., key: ...], [keys: [{id, key}, ...]], " <>
                "[key_for: fun, active: id] or false"
    end
  end

  defp do_normalize(_other) do
    raise ArgumentError,
          "encryption: expected a keyword list (see Sediment.S3, \"Encryption\") or false"
  end

  defp entries(entries, cipher) when is_list(entries) do
    entries
    |> Enum.with_index(1)
    |> Enum.map(fn {entry, n} -> entry(entry, cipher, "ring entry #{n}") end)
  end

  defp entries(_other, _cipher),
    do: raise(ArgumentError, "encryption: :keys is a list of {id, hex_key} entries")

  defp entry({id, key}, cipher, at), do: {id!(id, at), cipher, key!(key, at)}

  defp entry({id, key, opts}, cipher, at) when is_list(opts),
    do: {id!(id, at), cipher!(Keyword.get(opts, :cipher, cipher), at), key!(key, at)}

  defp entry(_other, _cipher, at) do
    raise ArgumentError,
          "encryption: #{at}: expected {id, hex_key} or {id, hex_key, cipher: c}"
  end

  defp id!(id, _at) when is_binary(id), do: id
  defp id!(id, _at) when is_atom(id) and id not in [nil, true, false], do: Atom.to_string(id)
  defp id!(_id, at), do: raise(ArgumentError, "encryption: #{at}: the id must be a string")

  defp key!(key, _at) when is_binary(key), do: key
  defp key!(_key, at), do: raise(ArgumentError, "encryption: #{at}: the key must be a hex string")

  defp cipher!(cipher, _at) when is_binary(cipher), do: cipher

  defp cipher!(cipher, _at) when is_atom(cipher) and cipher not in [nil, true, false],
    do: Atom.to_string(cipher)

  defp cipher!(_cipher, at),
    do: raise(ArgumentError, "encryption: #{at}: the cipher must be a string")

  defp key_for(nil), do: nil
  defp key_for(fun) when is_function(fun, 1), do: fun
  defp key_for({m, f, a}) when is_atom(m) and is_atom(f) and is_list(a), do: {m, f, a}

  defp key_for(_other),
    do: raise(ArgumentError, "encryption: :key_for is a 1-arity function or {module, fun, args}")

  @doc false
  # What the native code takes: the ring without its callback.
  @spec native(ring() | false | nil) :: map() | nil
  def native(nil), do: nil
  def native(false), do: nil

  def native(ring),
    do: %{keys: ring.keys, active: ring.active, resolvable: ring.key_for != nil}

  @doc false
  # Runs `fun` with the native ring, asking `key_for` for a key it needs
  # and running it again, at most 3 times. Once key_for has no key for an
  # id (or after the third), the native code names what is missing.
  @spec run(ring() | false | nil, (map() | nil -> result)) :: result when result: term()
  def run(ring, fun), do: run(ring, fun, @attempts)

  defp run(ring, fun, attempts) do
    case scrubbed(fn -> fun.(native(ring)) end) do
      {:error, {:key_needed, id}} when attempts > 0 and is_map(ring) ->
        run(resolve(ring, id), fun, attempts - 1)

      {:error, {:key_needed, _id}} when is_map(ring) ->
        scrubbed(fn -> fun.(native(%{ring | key_for: nil})) end)

      other ->
        other
    end
  end

  @doc false
  # The ring with the key `id` added through key_for, unless it has it.
  @spec with_key(ring(), String.t()) :: ring()
  def with_key(ring, id) do
    if List.keymember?(ring.keys, id, 0), do: ring, else: resolve(ring, id)
  end

  defp resolve(%{key_for: nil} = ring, _id), do: ring

  defp resolve(ring, id) do
    case call(ring.key_for, id) do
      key when is_binary(key) -> %{ring | keys: ring.keys ++ [{id, ring.cipher, key}]}
      {:ok, key} when is_binary(key) -> %{ring | keys: ring.keys ++ [{id, ring.cipher, key}]}
      # No such key: without the callback, the native code names the id.
      _ -> %{ring | key_for: nil}
    end
  end

  # A key_for that fails is reported by the id and the exception's type
  # only: its exception and its stack frames' arguments may hold keys (a
  # resolver's map of them, say).
  defp call(key_for, id) do
    apply_key_for(key_for, id)
  rescue
    exception ->
      reraise key_for_failed(id, "raised #{inspect(exception.__struct__)}"),
              Enum.map(__STACKTRACE__, &scrub_frame/1)
  catch
    kind, _reason ->
      reraise key_for_failed(id, "#{kind}"), Enum.map(__STACKTRACE__, &scrub_frame/1)
  end

  defp apply_key_for(fun, id) when is_function(fun, 1), do: fun.(id)
  defp apply_key_for({m, f, a}, id), do: apply(m, f, [id | a])

  defp key_for_failed(id, how) do
    %RuntimeError{
      message:
        "encryption: key_for failed for key #{shown(id)} (#{how}; details withheld: " <>
          "they may hold keys)"
    }
  end

  @doc false
  # An id as messages show it: as is, unless it could be a key (32 or more
  # hex digits; the native code refuses such ids).
  @spec shown(String.t()) :: String.t()
  def shown(id) do
    if String.match?(id, ~r/\A[0-9a-fA-F]{32,}\z/), do: "<an id that looks like a key>", else: id
  end

  # Normalization's own errors are ArgumentErrors starting "encryption: ";
  # anything else raised meanwhile (a value of an unexpected type, say) may
  # show the value, so it is replaced, and every frame's arguments dropped.
  defp sealed(fun) do
    fun.()
  rescue
    exception ->
      safe =
        case exception do
          %ArgumentError{message: "encryption: " <> _} ->
            exception

          other ->
            %ArgumentError{
              message:
                "encryption: invalid option (#{inspect(other.__struct__)}; details withheld: " <>
                  "it may hold keys)"
            }
        end

      reraise safe, Enum.map(__STACKTRACE__, &scrub_frame/1)
  end

  @doc false
  # Runs `fun`. An exception raised in it is raised again with every stack
  # frame's arguments replaced by their count: a NIF that can't decode an
  # argument raises badarg with all its arguments (key rings, S3
  # credentials) in the top frame, which consoles, crash reports and error
  # trackers would show. Exceptions that carry their arguments are replaced
  # by an ArgumentError naming only their type.
  @spec scrubbed((-> result)) :: result when result: term()
  def scrubbed(fun) do
    fun.()
  rescue
    exception -> reraise scrub(exception), Enum.map(__STACKTRACE__, &scrub_frame/1)
  end

  defp scrub(%ArgumentError{} = exception), do: exception
  defp scrub(%ErlangError{original: original} = exception) when is_atom(original), do: exception

  defp scrub(exception) do
    %ArgumentError{
      message:
        "#{inspect(exception.__struct__)} (details withheld: the arguments may hold keys or credentials)"
    }
  end

  defp scrub_frame({module, fun, args, location}) when is_list(args),
    do: {module, fun, length(args), location}

  defp scrub_frame(frame), do: frame
end
