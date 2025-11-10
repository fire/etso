defmodule Etso.ETS.Join.ResultMapper do
  @moduledoc """
  Handles mapping and structuring of join results, including select clauses and limits.
  """

  alias Etso.ETS.Join.FieldExtractor

  @doc """
  Maps results according to the select clause.
  """
  @spec map_to_select([tuple() | list()], Ecto.Query.t(), %{non_neg_integer() => {atom(), module()}}) :: [tuple() | list()]
  def map_to_select(results, query, sources) do
    case query.select do
      nil ->
        # No select - return all fields
        results
      %Ecto.Query.SelectExpr{fields: fields} ->
        # Map results according to select fields
        # Build join order: [0 (from), 1 (first join), 2 (second join), ...]
        join_order = [0 | Enum.map(query.joins, fn join ->
          join_source = case join.source do
            %Ecto.Query.JoinExpr{source: source} -> source
            _ -> join.source
          end
          find_source_index(join_source, sources)
        end)]
        Enum.map(results, fn tuple ->
          map_tuple_to_select_fields(tuple, fields, sources, join_order)
        end)
      _ ->
        results
    end
  end

  @doc """
  Structures join results according to Ecto's expectations.
  """
  @spec structure([tuple() | list()], Ecto.Query.t(), %{non_neg_integer() => {atom(), module()}}) :: [tuple() | list()]
  def structure(results, query, _sources) do
    # For joins, Ecto's postprocessor expects results as lists
    # But if select is a tuple, we need to return tuples
    # Check if select is a tuple structure
    is_tuple_select = case query.select do
      %Ecto.Query.SelectExpr{fields: fields} ->
        # Check if the first field is part of a tuple
        case fields do
          [{:{}, _, _} | _] -> true  # Tuple in AST
          _ -> false
        end
      _ -> false
    end
    
    Enum.map(results, fn
      tuple when is_tuple(tuple) ->
        if is_tuple_select do
          tuple  # Keep as tuple for tuple selects
        else
          Tuple.to_list(tuple)  # Convert to list for other selects
        end
      list when is_list(list) ->
        if is_tuple_select and length(list) > 0 do
          List.to_tuple(list)  # Convert to tuple for tuple selects
        else
          list  # Keep as list
        end
      other -> other
    end)
  end

  @doc """
  Extracts the limit value from a query limit expression.
  """
  @spec extract_limit(term() | nil, keyword()) :: non_neg_integer() | nil
  def extract_limit(nil, _params), do: nil
  
  # Handle LimitExpr struct (Ecto 3.11+)
  # LimitExpr has an `expr` field, not `count`
  def extract_limit(%{__struct__: struct_name, expr: expr}, params) when struct_name in [Ecto.Query.LimitExpr, "Elixir.Ecto.Query.LimitExpr"] do
    extract_limit_expr(expr, params) || find_integer_in_ast(expr)
  end
  
  # Fallback for older LimitExpr structure with `count` field
  def extract_limit(%{__struct__: struct_name, count: count}, params) when struct_name in [Ecto.Query.LimitExpr, "Elixir.Ecto.Query.LimitExpr"] do
    extract_limit_expr(count, params) || find_integer_in_ast(count)
  end
  
  def extract_limit(%Ecto.Query.QueryExpr{expr: expr}, params) do
    # Try to extract from the expression using all methods
    extract_limit_expr(expr, params) || find_integer_in_ast(expr)
  end
  
  # Handle direct integer limit
  def extract_limit(value, _params) when is_integer(value) do
    value
  end
  
  def extract_limit(limit, params) do
    # Last resort: try to find any integer in the structure
    # First try extract_limit_expr in case it's a nested structure
    case extract_limit_expr(limit, params) do
      nil -> find_integer_in_ast(limit)
      value -> value
    end
  end

  @spec map_tuple_to_select_fields(tuple() | list(), [term()], %{non_neg_integer() => {atom(), module()}}, [non_neg_integer()]) :: list()
  defp map_tuple_to_select_fields(tuple, fields, sources, all_source_indices) do
    # Extract values for each field in the select clause
    values = Enum.map(fields, fn field_expr ->
      extract_select_field_value(field_expr, tuple, sources, all_source_indices)
    end)
    
    # Return as list (will be converted to tuple if needed by structure_join_results)
    values
  end

  @spec extract_select_field_value(term(), tuple() | list(), %{non_neg_integer() => {atom(), module()}}, [non_neg_integer()]) :: term()
  defp extract_select_field_value({{:., _, [{:&, _, [source_idx]}, field_name]}, _, _}, tuple, sources, all_source_indices) do
    FieldExtractor.extract_field_from_combined_tuple(tuple, field_name, source_idx, sources, all_source_indices)
  end

  defp extract_select_field_value({:., _, [{:&, _, [source_idx]}, field_name]}, tuple, sources, all_source_indices) do
    FieldExtractor.extract_field_from_combined_tuple(tuple, field_name, source_idx, sources, all_source_indices)
  end

  defp extract_select_field_value({:json_extract_path, _, [field, path]}, tuple, sources, all_source_indices) do
    # Handle JSON extract paths
    base_value = extract_select_field_value(field, tuple, sources, all_source_indices)
    extract_json_path(base_value, path)
  end

  defp extract_select_field_value(_expr, _tuple, _sources, _all_source_indices) do
    # Handle computed fields, aggregates, etc.
    nil  # Simplified - would need full expression evaluation
  end

  @spec extract_json_path(term(), [atom() | binary() | integer()]) :: term()
  defp extract_json_path(value, path) when is_map(value) do
    Enum.reduce(path, value, fn
      key, acc when is_atom(key) or is_binary(key) -> Map.get(acc, key)
      index, acc when is_list(acc) -> Enum.at(acc, index)
      _, _ -> nil
    end)
  end

  defp extract_json_path(_, _), do: nil

  @spec extract_limit_expr(term(), keyword()) :: non_neg_integer() | nil
  defp extract_limit_expr({:^, _, [param_idx]}, params) do
    Enum.at(params, param_idx)
  end
  
  defp extract_limit_expr(value, _params) when is_integer(value) and value >= 0 do
    value
  end
  
  defp extract_limit_expr(value, _params) when is_integer(value) do
    nil  # Ignore negative limits
  end
  
  # Handle AST forms like {:., [], [5]} or similar
  defp extract_limit_expr({:., _, [value]}, _params) when is_integer(value) do
    value
  end
  
  defp extract_limit_expr({:., _, [{:^, _, [param_idx]}]}, params) do
    Enum.at(params, param_idx)
  end
  
  # Handle limit as a list with a single integer (Ecto sometimes wraps it)
  defp extract_limit_expr([value], _params) when is_integer(value) do
    value
  end
  
  # Handle limit as {:limit, _, [value]} or other AST forms
  defp extract_limit_expr({:limit, _, [value]}, _params) when is_integer(value) do
    value
  end
  
  defp extract_limit_expr({:limit, _, [{:^, _, [param_idx]}]}, params) do
    Enum.at(params, param_idx)
  end
  
  # Handle {:limit, _, args} where args might be a list
  defp extract_limit_expr({:limit, _, args}, params) when is_list(args) do
    case args do
      [value] when is_integer(value) -> value
      [{:^, _, [param_idx]}] -> Enum.at(params, param_idx)
      _ -> nil
    end
  end
  
  # Try to extract from nested structures - recursively check arguments
  defp extract_limit_expr(expr, params) when is_tuple(expr) do
    case expr do
      {_op, _meta, args} when is_list(args) ->
        # Try to extract from each argument
        Enum.find_value(args, fn arg ->
          result = extract_limit_expr(arg, params)
          if result, do: result, else: nil
        end) || find_integer_in_ast(expr)
      _ -> find_integer_in_ast(expr)
    end
  end
  
  # Fallback: try to find any integer in the expression
  defp extract_limit_expr(expr, _params) do
    find_integer_in_ast(expr)
  end
  
  # Improved helper to recursively find any integer in the AST
  # Collect all integers and return the first positive one (limits are positive)
  @spec find_integer_in_ast(term()) :: non_neg_integer() | nil
  defp find_integer_in_ast(expr) when is_integer(expr) and expr > 0, do: expr
  defp find_integer_in_ast(expr) when is_integer(expr), do: nil
  defp find_integer_in_ast(expr) when is_tuple(expr) do
    case expr do
      {_op, _meta, args} when is_list(args) ->
        # Check all arguments recursively first
        result = Enum.find_value(args, &find_integer_in_ast/1)
        if result do
          result
        else
          # Check if any element in the tuple is a positive integer
          (try do
            Enum.find_value(0..(tuple_size(expr) - 1), fn i ->
              val = elem(expr, i)
              if is_integer(val) and val > 0, do: val, else: nil
            end)
          rescue
            _ -> nil
          end)
        end
      _ -> 
        # Check if any element in the tuple is a positive integer
        (try do
          Enum.find_value(0..(tuple_size(expr) - 1), fn i ->
            val = elem(expr, i)
            if is_integer(val) and val > 0, do: val, else: nil
          end)
        rescue
          _ -> nil
        end)
    end
  end
  defp find_integer_in_ast([head | _]) when is_integer(head) and head > 0, do: head
  defp find_integer_in_ast([head | tail]) when is_list(tail) do
    find_integer_in_ast(head) || find_integer_in_ast(tail)
  end
  defp find_integer_in_ast(_), do: nil

  @spec find_source_index({atom(), module()}, %{non_neg_integer() => {atom(), module()}}) :: non_neg_integer() | nil
  defp find_source_index(source, sources) do
    Enum.find_value(sources, fn {idx, s} -> if s == source, do: idx end)
  end
end

