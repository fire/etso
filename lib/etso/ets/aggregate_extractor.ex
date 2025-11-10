defmodule Etso.ETS.AggregateExtractor do
  @moduledoc """
  The Aggregate Extractor module is responsible for extracting field values from ETS tuples
  for aggregate operations (count, sum, avg, min, max).
  """

  alias Etso.ETS.TableStructure

  @doc """
  Extracts field values from ETS tuples for aggregate computation.

  Returns a list of extracted values, handling nil values and type conversions.
  """
  def extract_values(ets_objects, schema, field_name, distinct \\ false) do
    field_names = TableStructure.field_names(schema)
    field_index = Enum.find_index(field_names, &(&1 == field_name))

    if field_index do
      values =
        Enum.map(ets_objects, fn
          tuple when is_tuple(tuple) ->
            # ETS tuples are 1-indexed
            # field_index is 0-indexed, so we add 1
            elem(tuple, field_index + 1)
          list when is_list(list) ->
            # When match spec body returns a list, ETS returns a list of lists
            # field_index is 0-indexed
            Enum.at(list, field_index)
        end)

      if distinct do
        Enum.uniq(values)
      else
        values
      end
    else
      []
    end
  end

  @doc """
  Converts values to numeric types for aggregation.

  Handles integers, floats, and Decimal types.
  """
  def to_numeric(value) when is_integer(value), do: value
  def to_numeric(value) when is_float(value), do: value
  def to_numeric(%Decimal{} = value), do: Decimal.to_float(value)
  def to_numeric(nil), do: nil
  def to_numeric(_), do: nil

  @doc """
  Filters out nil values from a list.
  """
  def filter_nils(values) do
    Enum.reject(values, &is_nil/1)
  end
end

