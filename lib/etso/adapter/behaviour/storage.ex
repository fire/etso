defmodule Etso.Adapter.Behaviour.Storage do
  @moduledoc false
  @behaviour Ecto.Adapter.Storage

  @impl Ecto.Adapter.Storage
  def storage_up(_options) do
    # For ETS, storage is always "up" since tables are created on-demand
    # This is a no-op but returns success for migration compatibility
    :ok
  end

  @impl Ecto.Adapter.Storage
  def storage_down(_options) do
    # For ETS, we can't really "drop" storage since it's in-memory
    # But we return success for migration compatibility
    :ok
  end

  @impl Ecto.Adapter.Storage
  def storage_status(_options) do
    # ETS storage is always available
    :up
  end
end

