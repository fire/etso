defmodule Etso.ETS.Join.FieldExtractor do
  @moduledoc """
  Handles extraction of field values from tuples in join contexts.
  """

  alias Etso.ETS.TableStructure

  @doc """
  Extracts a field value from an AST expression in a join condition context.
  """
  @spec extract_field_value(term(), tuple(), tuple() | list(), [non_neg_integer()], non_neg_integer(), %{non_neg_integer() => {atom(), module()}}, keyword(), [non_neg_integer()]) :: term()
  def extract_field_value({{:., _, [{:&, _, [source_idx]}, field_name]}, _, _}, left_tuple, right_tuple, left_indices, right_idx, sources, _params, join_order) do
    extract_field_from_tuple(source_idx, field_name, left_tuple, right_tuple, left_indices, right_idx, sources, join_order)
  end

  def extract_field_value({:., _meta, [{:&, _, [source_idx]}, field_name]}, left_tuple, right_tuple, left_indices, right_idx, sources, _params, join_order) do
    extract_field_from_tuple(source_idx, field_name, left_tuple, right_tuple, left_indices, right_idx, sources, join_order)
  end
  

  def extract_field_value({:^, _, [param_idx]}, _, _, _, _, _, params, _join_order) do
    # Parameter reference in join conditions
    Enum.at(params, param_idx)
  end

  # Catch-all: try to recursively search for field access in AST structures
  def extract_field_value({_, _, args} = expr, left_tuple, right_tuple, left_indices, right_idx, sources, _params, join_order) when is_tuple(expr) and tuple_size(expr) == 3 do
    # This looks like an AST node - try to extract field from args
    if is_list(args) and length(args) > 0 do
      # Try to extract from the first argument if it looks like a field access
      case List.first(args) do
        {{:., _, [{:&, _, [source_idx]}, field_name]}, _, _} ->
          extract_field_from_tuple(source_idx, field_name, left_tuple, right_tuple, left_indices, right_idx, sources, join_order)
        {:., _, [{:&, _, [source_idx]}, field_name]} ->
          extract_field_from_tuple(source_idx, field_name, left_tuple, right_tuple, left_indices, right_idx, sources, join_order)
        _ ->
          # Fallback: return the value as-is
          expr
      end
    else
      expr
    end
  end

  def extract_field_value(value, _, _, _, _, _, _, _), do: value

  @spec extract_field_from_tuple(non_neg_integer(), atom(), tuple(), tuple() | list(), [non_neg_integer()], non_neg_integer(), %{non_neg_integer() => {atom(), module()}}, [non_neg_integer()]) :: term()
  def extract_field_from_tuple(source_idx, field_name, left_tuple, right_tuple, left_indices, right_idx, sources, join_order) do
    cond do
      source_idx in left_indices ->
        # Source is from left side (could be from a previous join)
        extract_field_from_combined_tuple(left_tuple, field_name, source_idx, sources, join_order)
      source_idx == right_idx ->
        extract_field_from_joined_tuple(right_tuple, field_name, sources[right_idx])
      true ->
        # Should not happen
        nil
    end
  end

  @spec extract_field_from_joined_tuple(tuple() | list(), atom(), {atom(), module()}) :: term()
  def extract_field_from_joined_tuple(tuple, field_name, {_, schema}) when is_tuple(tuple) do
    field_names = TableStructure.field_names(schema)
    field_index = Enum.find_index(field_names, &(&1 == field_name))
    # Elixir tuples are 0-indexed
    if field_index != nil, do: elem(tuple, field_index), else: nil
  end

  def extract_field_from_joined_tuple(list, field_name, {_, schema}) when is_list(list) do
    field_names = TableStructure.field_names(schema)
    field_index = Enum.find_index(field_names, &(&1 == field_name))
    if field_index != nil, do: Enum.at(list, field_index), else: nil
  end

  def extract_field_from_joined_tuple(_, _, _), do: nil

  @doc """
  Extracts a field value from a combined tuple (result of multiple joins).
  """
  @spec extract_field_from_combined_tuple(tuple() | list(), atom(), non_neg_integer(), %{non_neg_integer() => {atom(), module()}}, [non_neg_integer()]) :: term()
  def extract_field_from_combined_tuple(combined_tuple, field_name, source_idx, sources, join_order) when is_tuple(combined_tuple) do
    # For combined tuples from multiple joins, we need to find which position this source's fields start at
    # Calculate offset by summing field counts of all sources before this one in the join order
    offset = calculate_field_offset(source_idx, sources, join_order)
    {_, schema} = sources[source_idx]
    field_names = TableStructure.field_names(schema)
    field_index = Enum.find_index(field_names, &(&1 == field_name))
    
    # Elixir tuples are 0-indexed
    # offset is the number of fields before this source (0-indexed)
    # field_index is 0-based
    # Total: offset + field_index
    if field_index != nil do
      tuple_pos = offset + field_index
      # Check bounds to avoid ArgumentError
      if tuple_pos < tuple_size(combined_tuple) do
        elem(combined_tuple, tuple_pos)
      else
        nil
      end
    else
      nil
    end
  end

  def extract_field_from_combined_tuple(combined_list, field_name, source_idx, sources, join_order) when is_list(combined_list) do
    # Handle lists (after structure_join_results conversion)
    offset = calculate_field_offset(source_idx, sources, join_order)
    {_, schema} = sources[source_idx]
    field_names = TableStructure.field_names(schema)
    field_index = Enum.find_index(field_names, &(&1 == field_name))
    
    # Lists are 0-indexed
    if field_index, do: Enum.at(combined_list, offset + field_index), else: nil
  end

  def extract_field_from_combined_tuple(_, _, _, _, _), do: nil

  @spec calculate_field_offset(non_neg_integer(), %{non_neg_integer() => {atom(), module()}}, [non_neg_integer()]) :: non_neg_integer()
  def calculate_field_offset(target_idx, sources, join_order) do
    # Sum field counts of all sources that come before target_idx in join_order
    # join_order is the order in which sources appear in the combined tuple: [0, 1, 2, ...]
    join_order
    |> Enum.take_while(&(&1 != target_idx))
    |> Enum.reduce(0, fn idx, acc ->
      acc + get_field_count_for_source(sources, idx)
    end)
  end

  @spec get_field_count_for_source(%{non_neg_integer() => {atom(), module()}}, non_neg_integer()) :: non_neg_integer()
  defp get_field_count_for_source(sources, source_idx) do
    case sources[source_idx] do
      {_, schema} -> length(TableStructure.field_names(schema))
      _ -> 0
    end
  end
end

