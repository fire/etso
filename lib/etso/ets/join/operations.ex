defmodule Etso.ETS.Join.Operations do
  @moduledoc """
  Implements the core join operations: inner, left, right, and full outer joins.
  """

  alias Etso.ETS.TableStructure

  @doc """
  Performs an inner join between left and right results.
  """
  @spec inner_join(
    [tuple() | list()],
    [tuple() | list()],
    (tuple(), tuple() | list(), [non_neg_integer()], non_neg_integer() -> boolean()),
    %{non_neg_integer() => {atom(), module()}},
    [non_neg_integer()],
    non_neg_integer()
  ) :: [tuple()]
  def inner_join(left_results, right_results, condition_evaluator, _sources, left_indices, right_idx) do
    Enum.flat_map(left_results, fn left_tuple ->
      matching_right = Enum.filter(right_results, fn right_tuple ->
        condition_evaluator.(left_tuple, right_tuple, left_indices, right_idx)
      end)
      
      Enum.map(matching_right, fn right_tuple ->
        combine_tuples(left_tuple, right_tuple)
      end)
    end)
  end

  @doc """
  Performs a left join between left and right results.
  """
  @spec left_join(
    [tuple() | list()],
    [tuple() | list()],
    (tuple(), tuple() | list(), [non_neg_integer()], non_neg_integer() -> boolean()),
    %{non_neg_integer() => {atom(), module()}},
    [non_neg_integer()],
    non_neg_integer(),
    non_neg_integer()
  ) :: [tuple()]
  def left_join(left_results, right_results, condition_evaluator, _sources, left_indices, right_idx, right_field_count) do
    Enum.flat_map(left_results, fn left_tuple ->
      matching_right = Enum.filter(right_results, fn right_tuple ->
        condition_evaluator.(left_tuple, right_tuple, left_indices, right_idx)
      end)
      
      case matching_right do
        [] ->
          # No match - add nil values for right table
          nil_tuple = create_nil_tuple(right_field_count)
          [combine_tuples(left_tuple, nil_tuple)]
        _ ->
          Enum.map(matching_right, fn right_tuple ->
            combine_tuples(left_tuple, right_tuple)
          end)
      end
    end)
  end

  @doc """
  Performs a right join between left and right results.
  """
  @spec right_join(
    [tuple() | list()],
    [tuple() | list()],
    (tuple(), tuple() | list(), [non_neg_integer()], non_neg_integer() -> boolean()),
    %{non_neg_integer() => {atom(), module()}},
    [non_neg_integer()],
    non_neg_integer(),
    non_neg_integer()
  ) :: [tuple()]
  def right_join(left_results, right_results, condition_evaluator, sources, left_indices, right_idx, _right_field_count) do
    # Calculate total field count for all left sources
    left_field_count = Enum.reduce(left_indices, 0, fn idx, acc ->
      acc + get_field_count_for_source(sources, idx)
    end)
    
    Enum.flat_map(right_results, fn right_tuple ->
      matching_left = Enum.filter(left_results, fn left_tuple ->
        condition_evaluator.(left_tuple, right_tuple, left_indices, right_idx)
      end)
      
      case matching_left do
        [] ->
          # No match - add nil values for left table(s)
          nil_tuple = create_nil_tuple(left_field_count)
          [combine_tuples(nil_tuple, right_tuple)]
        _ ->
          Enum.map(matching_left, fn left_tuple ->
            combine_tuples(left_tuple, right_tuple)
          end)
      end
    end)
  end

  @doc """
  Performs a full outer join between left and right results.
  """
  @spec full_join(
    [tuple() | list()],
    [tuple() | list()],
    (tuple(), tuple() | list(), [non_neg_integer()], non_neg_integer() -> boolean()),
    %{non_neg_integer() => {atom(), module()}},
    [non_neg_integer()],
    non_neg_integer(),
    non_neg_integer()
  ) :: [tuple()]
  def full_join(left_results, right_results, condition_evaluator, sources, left_indices, right_idx, right_field_count) do
    # Calculate total field count for all left sources
    left_field_count = Enum.reduce(left_indices, 0, fn idx, acc ->
      acc + get_field_count_for_source(sources, idx)
    end)
    
    # Left join part
    left_joined = left_join(left_results, right_results, condition_evaluator, sources, left_indices, right_idx, right_field_count)
    
    # Right-only part (rows in right with no match in left)
    right_only = Enum.flat_map(right_results, fn right_tuple ->
      has_match = Enum.any?(left_results, fn left_tuple ->
        condition_evaluator.(left_tuple, right_tuple, left_indices, right_idx)
      end)
      
      if not has_match do
        nil_tuple = create_nil_tuple(left_field_count)
        [combine_tuples(nil_tuple, right_tuple)]
      else
        []
      end
    end)
    
    left_joined ++ right_only
  end

  @doc """
  Combines two tuples or lists into a single tuple.
  """
  @spec combine_tuples(tuple() | list(), tuple() | list()) :: tuple()
  def combine_tuples(left_tuple, right_tuple) when is_tuple(left_tuple) and is_tuple(right_tuple) do
    left_list = Tuple.to_list(left_tuple)
    right_list = Tuple.to_list(right_tuple)
    List.to_tuple(left_list ++ right_list)
  end

  def combine_tuples(left_list, right_list) when is_list(left_list) and is_list(right_list) do
    # Handle case where ETS returns lists instead of tuples
    List.to_tuple(left_list ++ right_list)
  end

  def combine_tuples(left, right) do
    # Convert to lists if needed, then combine
    left_list = if is_tuple(left), do: Tuple.to_list(left), else: left
    right_list = if is_tuple(right), do: Tuple.to_list(right), else: right
    List.to_tuple(left_list ++ right_list)
  end

  @spec create_nil_tuple(non_neg_integer()) :: tuple()
  defp create_nil_tuple(field_count) do
    List.to_tuple(List.duplicate(nil, field_count))
  end

  @spec get_field_count_for_source(%{non_neg_integer() => {atom(), module()}}, non_neg_integer()) :: non_neg_integer()
  defp get_field_count_for_source(sources, source_idx) do
    case sources[source_idx] do
      {_, schema} -> length(TableStructure.field_names(schema))
      _ -> 0
    end
  end
end

