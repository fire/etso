defmodule Etso.ETS.TableStructure do
  @moduledoc """
  The ETS Table Structure module contains various convenience functions to aid the transformation
  between Ecto Schemas (maps) and ETS entries (tuples). The primary key is moved to the head, in
  accordance with ETS conventions. Supports both single and composite primary keys.
  """

  def field_names(schema) do
    fields = schema.__schema__(:fields)
    primary_key = schema.__schema__(:primary_key)
    primary_key ++ (fields -- primary_key)
  end

  def fields_to_tuple(field_names, fields) do
    values = Enum.map(field_names, &Keyword.get(fields, &1, nil))
    List.to_tuple(values)
  end

  def entries_to_tuples(field_names, entries) do
    for entry <- entries do
      fields_to_tuple(field_names, entry)
    end
  end

  @doc """
  Extracts the primary key value(s) from a fields keyword list.
  Returns a single value for single primary keys, or a tuple for composite keys.
  """
  def extract_primary_key(schema, fields) do
    primary_keys = schema.__schema__(:primary_key)
    key_values = Enum.map(primary_keys, &Keyword.get(fields, &1))

    case primary_keys do
      [_] -> hd(key_values)
      _ -> List.to_tuple(key_values)
    end
  end

  @doc """
  Extracts the primary key value(s) from an ETS tuple.
  Returns a single value for single primary keys, or a tuple for composite keys.
  """
  def extract_primary_key_from_tuple(schema, tuple) do
    primary_keys = schema.__schema__(:primary_key)
    field_names = field_names(schema)

    key_indices = Enum.map(primary_keys, fn key ->
      Enum.find_index(field_names, &(&1 == key))
    end)

    key_values = Enum.map(key_indices, fn index ->
      elem(tuple, index + 1)
    end)

    case primary_keys do
      [_] -> hd(key_values)
      _ -> List.to_tuple(key_values)
    end
  end
end
