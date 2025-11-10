defmodule Etso.Adapter.Behaviour.Locking do
  @moduledoc false
  # Note: Ecto doesn't have a formal Locking behaviour, but locking
  # (SELECT ... FOR UPDATE) is handled through query options.
  # This module provides utilities for processing locking in queries.

  @doc """
  Processes locking options in a query.
  For ETS, locking is a no-op since ETS operations are atomic,
  but we provide this for API compatibility.
  """
  @spec process_locking(Ecto.Query.t(), keyword()) :: Ecto.Query.t()
  def process_locking(query, _opts) do
    # ETS operations are already atomic, so locking is a no-op
    # But we return the query unchanged for compatibility
    query
  end

  @doc """
  Checks if a query has locking enabled.
  """
  @spec has_locking?(Ecto.Query.t()) :: boolean()
  def has_locking?(query) do
    case query.lock do
      nil -> false
      _ -> true
    end
  end
end

