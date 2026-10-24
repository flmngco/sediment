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
  # `term` with every `:encryption` in it (of a map or a keyword list, at any
  # depth but in structs) hidden: a target as the caller's functions get it
  # back, its rings still usable to reopen a database.
  @spec hide_rings(term()) :: term()
  def hide_rings(list) when is_list(list), do: hide_rings_list(list)

  def hide_rings(tuple) when is_tuple(tuple),
    do: tuple |> Tuple.to_list() |> hide_rings() |> List.to_tuple()

  def hide_rings(%_{} = struct), do: struct

  def hide_rings(map) when is_map(map),
    do: Map.new(map, fn {k, v} -> hide_ring({k, v}) end)

  def hide_rings(other), do: other

  defp hide_ring({:encryption, encryption}), do: {:encryption, hide(encryption)}
  defp hide_ring({k, v}), do: {k, hide_rings(v)}

  defp hide_rings_list([{_, _} = head | tail]) when is_list(tail),
    do: [hide_ring(head) | hide_rings_list(tail)]

  defp hide_rings_list([head | tail]) when is_list(tail),
    do: [hide_rings(head) | hide_rings_list(tail)]

  defp hide_rings_list([head | tail]), do: [hide_rings(head) | hide_rings(tail)]
  defp hide_rings_list([]), do: []

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
  # The boundary every public function of Sediment runs in (see
  # Sediment.S3.Keys.Sealed), from its argument handling to its NIF calls:
  #
  #   * an exception raised meanwhile loses its stack frames' arguments (a
  #     function clause that doesn't match a map of options, or a NIF that
  #     can't decode an argument, puts them all, key rings and S3
  #     credentials included, in the top frame, which consoles, crash
  #     reports and error trackers would show);
  #   * only Sediment's own exceptions (see `passes?/2`) keep their message;
  #     any other (a FunctionClauseError, KeyError or MatchError holds the
  #     value it failed on) becomes an ArgumentError naming only its type;
  #   * the net: in what passes (an exception, an exit's reason, an
  #     `{:error, reason}` result), every run of 32 or more hex digits in
  #     its text is replaced by "<redacted>". Keys are hex, so a key echoed
  #     anywhere, now or by a future message, never leaves. Nothing Sediment
  #     shows legitimately is such a run (fingerprints show 16 hex digits).
  #     Only text is rewritten (see `redact/1`): a struct other than an
  #     exception, such as the connection state DBConnection keeps from a
  #     callback's `{:error, state}`, is returned as it is. Of an
  #     `{:error | :disconnect, exception, state}` result only the exception
  #     is.
  #
  # Throws pass untouched (a transaction's rollback is one).
  @spec scrubbed((-> result)) :: result when result: term()
  def scrubbed(fun) do
    case fun.() do
      {:error, reason} ->
        {:error, redact(reason)}

      {status, %{__exception__: true} = e, state} when status in [:error, :disconnect] ->
        {status, redact(e), state}

      result ->
        result
    end
  rescue
    exception ->
      stacktrace = __STACKTRACE__
      safe = if passes?(exception, stacktrace), do: redact(exception), else: withheld(exception)
      reraise safe, Enum.map(stacktrace, &scrub_frame/1)
  catch
    :exit, reason -> exit(redact(reason))
  end

  @doc false
  # `term` with every run of 32 or more hex digits in its text (strings and
  # atoms, in lists, tuples, maps and exceptions' fields) replaced by
  # "<redacted>", its shape kept. Any other struct (a connection's state, a
  # query, a result) is data a caller or DBConnection keeps, not a message,
  # and is returned as it is.
  @spec redact(term()) :: term()
  def redact(binary) when is_binary(binary),
    do: Regex.replace(~r/[0-9a-fA-F]{32,}/, binary, "<redacted>")

  def redact(atom) when is_atom(atom) and atom not in [nil, true, false] do
    string = Atom.to_string(atom)

    case redact(string) do
      ^string -> atom
      redacted -> String.to_atom(redacted)
    end
  end

  def redact(list) when is_list(list), do: redact_list(list)

  def redact(tuple) when is_tuple(tuple),
    do: tuple |> Tuple.to_list() |> redact() |> List.to_tuple()

  # Its statement is the caller's, kept for them; message/1 and inspect/2
  # show it redacted.
  def redact(%Sediment.Error{} = error), do: %{error | message: redact(error.message)}

  def redact(%module{__exception__: true} = exception) do
    exception |> Map.from_struct() |> redact() |> Map.put(:__struct__, module)
  end

  def redact(%_{} = struct), do: struct
  def redact(map) when is_map(map), do: Map.new(map, fn {k, v} -> {redact(k), redact(v)} end)
  def redact(other), do: other

  defp redact_list([head | tail]) when is_list(tail), do: [redact(head) | redact_list(tail)]
  defp redact_list([head | tail]), do: [redact(head) | redact(tail)]
  defp redact_list([]), do: []

  @doc false
  # A value a caller gave, as a message names it (a mode, a parameter, a
  # repo, a process name): an atom, a string or a number as inspect shows
  # it, unless it could be a key (a run of 32 or more hex digits); anything
  # else by its type only (a term can hold a key anywhere).
  @spec shown_value(term()) :: String.t()
  def shown_value(term)
      when is_atom(term) or is_binary(term) or is_number(term) or is_pid(term) or
             is_reference(term) or is_port(term) do
    shown = inspect(term)
    if shown =~ ~r/[0-9a-fA-F]{32,}/, do: "a value that looks like a key", else: shown
  end

  def shown_value(%module{}), do: "%#{module |> shown_value() |> String.trim_leading(":")}{...}"
  def shown_value(term), do: "a value of type #{type(term)}"

  defp type(term) when is_list(term), do: "list"
  defp type(term) when is_map(term), do: "map"
  defp type(term) when is_tuple(term), do: "tuple"
  defp type(term) when is_function(term), do: "function"
  defp type(_term), do: "term"

  # Sediment's own exceptions, whose messages show no value a caller gave
  # (and pass through the net anyway): a key ring's errors (normalization,
  # a failed key_for), the ArgumentErrors and RuntimeErrors raised in a
  # module of Sediment's (a NIF's badarg is raised in Sediment.Native, as
  # "argument error") or of Ecto's, Sediment.Error, and DBConnection's errors, which
  # callers handle by their type.
  @passing [
    Sediment.Error,
    DBConnection.ConnectionError,
    DBConnection.TransactionError,
    DBConnection.OwnershipError
  ]

  defp passes?(%type{message: "encryption: " <> _}, _stacktrace)
       when type in [ArgumentError, RuntimeError],
       do: true

  defp passes?(%ErlangError{original: original}, _stacktrace) when is_atom(original), do: true
  defp passes?(%type{}, _stacktrace) when type in @passing, do: true

  defp passes?(%type{} = exception, [{module, _fun, _args, _location} | _])
       when type in [ArgumentError, RuntimeError],
       do: withheld?(exception) or sediment_module?(module)

  defp passes?(_exception, _stacktrace), do: false

  # Sediment's modules, and Ecto's: Ecto's ArgumentErrors and RuntimeErrors
  # say why a repo didn't start (a `log: :verbose`, a driver missing), and
  # name options, not their values; the net still covers them.
  defp sediment_module?(module) do
    case Atom.to_string(module) do
      "Elixir.Ecto." <> _ -> true
      "Elixir." <> _ -> "Sediment" in Module.split(module)
      _erlang -> false
    end
  end

  @withheld "(details withheld: the arguments may hold keys or credentials)"

  defp withheld(exception),
    do: %ArgumentError{message: "#{inspect(exception.__struct__)} #{@withheld}"}

  # An exception an inner boundary withheld already.
  defp withheld?(%ArgumentError{message: message}), do: String.ends_with?(message, @withheld)
  defp withheld?(_exception), do: false

  defmodule Caller do
    @moduledoc false
    # What a caller's function returned, threw or raised, carried through
    # the boundary untouched (see Sediment.S3.Keys.around/2).
    defstruct [:value, :kind, :stacktrace]
  end

  @doc false
  # `run` (a library's setup and completion around the caller's function
  # `fun`, such as DBConnection.transaction/3) in the boundary, `fun` out of
  # it: what `fun` returns, its rollback reason (DBConnection.rollback/2),
  # its exceptions, exits and throws reach the caller as they are, with
  # their stack traces. `run` gets `fun` wrapped, a function of one argument.
  @spec around((term() -> term()), ((term() -> term()) -> result)) :: result
        when result: term()
  def around(fun, run) do
    case scrubbed(fn -> run.(carried(fun)) end) do
      {:ok, %Caller{kind: nil, value: value}} -> {:ok, value}
      {:error, %Caller{kind: nil, value: reason}} -> {:error, reason}
      result -> result
    end
  catch
    :throw, %Caller{kind: kind, value: reason, stacktrace: stacktrace} ->
      :erlang.raise(kind, reason, stacktrace)
  end

  defp carried(fun) do
    fn %{conn_ref: conn_ref} = conn ->
      try do
        %Caller{value: fun.(conn)}
      catch
        # A rollback of this transaction: DBConnection ends it with the
        # (carried) reason.
        :throw, {DBConnection, ^conn_ref, reason} ->
          throw({DBConnection, conn_ref, %Caller{value: reason}})

        # Of another transaction, or carried already (a nested one's).
        :throw, {DBConnection, _ref, _reason} = rollback ->
          throw(rollback)

        :throw, %Caller{} = carried ->
          throw(carried)

        kind, reason ->
          throw(%Caller{kind: kind, value: reason, stacktrace: __STACKTRACE__})
      end
    end
  end

  @ring_options [:key, :keys, :key_for, :cipher, :active]

  @doc false
  # The options of a public function taking keys, checked before anything
  # reads them, with fixed messages: a keyword list, without a key ring's
  # own options at the top level (a ring given in place of the options
  # would otherwise be ignored, opening an unencrypted database). `except`:
  # ring options the caller refuses itself.
  @spec options(term(), String.t(), [atom()]) :: :ok | {:error, String.t()}
  def options(opts, call, except \\ []) do
    cond do
      not keyword?(opts) ->
        {:error, "#{call}: the options must be a keyword list"}

      option = Enum.find(@ring_options -- except, &Keyword.has_key?(opts, &1)) ->
        {:error,
         "#{call}: #{inspect(option)} is an option of the key ring, given as " <>
           "encryption: [...] (see Sediment.S3, \"Encryption\")"}

      true ->
        :ok
    end
  end

  @doc false
  @spec options!(term(), String.t(), [atom()]) :: keyword()
  def options!(opts, call, except \\ []) do
    case options(opts, call, except) do
      :ok -> opts
      {:error, message} -> raise ArgumentError, message
    end
  end

  defp keyword?([]), do: true
  defp keyword?([{key, _value} | rest]) when is_atom(key), do: keyword?(rest)
  defp keyword?(_other), do: false

  @doc false
  # A stack trace without its frames' arguments (see scrubbed/1).
  @spec scrub_stacktrace(Exception.stacktrace()) :: Exception.stacktrace()
  def scrub_stacktrace(stacktrace), do: Enum.map(stacktrace, &scrub_frame/1)

  # A frame's module and function are atoms a caller can have given too
  # (a key as a module, called).
  defp scrub_frame({module, fun, args, location}) when is_list(args),
    do: {redact(module), redact(fun), length(args), location}

  defp scrub_frame({module, fun, arity, location}),
    do: {redact(module), redact(fun), arity, location}

  defp scrub_frame(frame), do: frame
end
