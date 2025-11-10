defmodule Etso.ETS.JoinExecutor do
  @moduledoc """
  The Join Executor module is responsible for executing join queries in-memory.
  It supports inner, left, right, and full outer joins with complex conditions.
  """

  alias Etso.Adapter.TableRegistry
  alias Etso.ETS.MatchSpecification
  alias Etso.ETS.ObjectsSorter
  alias Etso.ETS.TableStructure

  @doc """
  Executes a join query by fetching data from each table and performing in-memory joins.
  """
  @spec execute(Etso.Adapter.Meta.t(), Ecto.Query.t(), keyword()) :: {non_neg_integer(), [tuple() | list()]}
  def execute(adapter_meta, query, params) do
    repo = get_repo(adapter_meta)
    
    # Build source map to track all joined tables and their indices
    sources = build_source_map(query)
    
    # Fetch data from the main table
    {_, from_schema} = query.from.source
    {:ok, from_table} = TableRegistry.get_table(repo, from_schema)
    
    # Build match spec for from table (only where clauses that apply ONLY to from table)
    # Remove select to avoid field resolution issues
    from_wheres = filter_wheres_for_source(query.wheres, 0, sources)
    from_query = %{query | joins: [], wheres: from_wheres, select: nil}
    from_match = MatchSpecification.build(from_query, params)
    from_objects = :ets.select(from_table, [from_match])
    
    # Process each join sequentially, building up the result set
    # Track which sources have been joined so far
    results = Enum.reduce(query.joins, {from_objects, [0]}, fn join, {acc, joined_indices} ->
      new_results = perform_join(adapter_meta, repo, query, join, acc, params, sources, joined_indices)
      # Extract source from join
      join_source = case join.source do
        %Ecto.Query.JoinExpr{source: source} -> source
        _ -> join.source
      end
      join_idx = find_source_index(join_source, sources)
      # Append to preserve join order (fields are combined in order: [0, 1, 2, ...])
      {new_results, joined_indices ++ [join_idx]}
    end)
    |> elem(0)
    
    # Build join order for field extraction: [0 (from), 1 (first join), 2 (second join), ...]
    join_order = [0 | Enum.map(query.joins, fn join ->
      join_source = case join.source do
        %Ecto.Query.JoinExpr{source: source} -> source
        _ -> join.source
      end
      find_source_index(join_source, sources)
    end)]
    
    # Apply where clauses that reference multiple tables (after all joins)
    # Note: Single-source wheres should already be applied via match specs
    # But we also need to apply single-source wheres that weren't applied earlier
    # (e.g., if they reference a source that was joined later)
    filtered_results = apply_all_wheres(results, query.wheres, sources, params, join_order)
    
    # Map results according to select clause if present (before sorting)
    # We need to map while we still have tuples for sorting
    mapped_results = if query.select do
      map_results_to_select(filtered_results, query, sources)
    else
      filtered_results
    end
    
    # Apply sorting (ObjectsSorter works on lists/tuples via Enum.at)
    # Convert tuples to lists for consistent handling
    sortable_results = Enum.map(mapped_results, fn
      list when is_list(list) -> list
      tuple when is_tuple(tuple) -> Tuple.to_list(tuple)
      other -> other
    end)
    sorted_results = ObjectsSorter.sort(sortable_results, query)
    
    # Convert tuples to lists (Ecto expects lists for join results)
    structured_results = structure_join_results(sorted_results, query, sources)
    
    # Apply limit if present
    limit_value = extract_limit_value(query.limit, params)
    
    limited_results = case limit_value do
      value when is_integer(value) and value > 0 ->
        Enum.take(structured_results, value)
      _ ->
        structured_results
    end
    
    {length(limited_results), limited_results}
  end

  @spec get_repo(Etso.Adapter.Meta.t()) :: module()
  defp get_repo(adapter_meta) do
    adapter_meta.dynamic_repo || adapter_meta.repo
  end

  @spec build_source_map(Ecto.Query.t()) :: %{non_neg_integer() => {atom(), module()}}
  defp build_source_map(query) do
    # Build a map of source_index -> {source_name, schema}
    # Source 0 is always the from table
    sources = %{0 => query.from.source}
    
    Enum.reduce(query.joins, {sources, 1}, fn join, {acc, idx} ->
      source = case join.source do
        %Ecto.Query.JoinExpr{source: source} -> source
        _ -> join.source
      end
      {Map.put(acc, idx, source), idx + 1}
    end)
    |> elem(0)
  end

  @spec perform_join(
    Etso.Adapter.Meta.t(),
    module(),
    Ecto.Query.t(),
    Ecto.Query.JoinExpr.t(),
    [tuple() | list()],
    keyword(),
    map(),
    [non_neg_integer()]
  ) :: [tuple() | list()]
  defp perform_join(_adapter_meta, repo, query, join, left_results, params, sources, joined_indices) do
    # Extract source from join - handle both JoinExpr and direct source
    join_source = case join.source do
      %Ecto.Query.JoinExpr{source: source} -> source
      _ -> join.source
    end
    
    {_, join_schema} = join_source
    {:ok, join_table} = TableRegistry.get_table(repo, join_schema)
    
    # Get the source index for this join
    join_source_idx = find_source_index(join_source, sources)
    
    # Build a query for the join table (only where clauses that apply to this join table)
    # Remove select and joins to avoid issues with field resolution
    # Remap source indices in where clauses to 0 (since this is now the from table)
    join_wheres = filter_wheres_for_source(query.wheres, join_source_idx, sources)
    remapped_wheres = remap_source_indices(join_wheres, join_source_idx, 0)
    join_query = %{query | from: %{query.from | source: join_source}, joins: [], wheres: remapped_wheres, select: nil}
    join_match = MatchSpecification.build(join_query, params)
    join_objects = :ets.select(join_table, [join_match])
    
    # Extract and build join condition evaluator
    # join.on can be either a BooleanExpr or a QueryExpr
    condition_expr = case join.on do
      %Ecto.Query.BooleanExpr{expr: expr} -> expr
      %Ecto.Query.QueryExpr{expr: expr} -> expr
      _ -> raise "Unexpected join.on type: #{inspect(join.on.__struct__)}"
    end
    condition_evaluator = build_condition_evaluator(%Ecto.Query.BooleanExpr{expr: condition_expr}, sources, params)
    
    # Get field counts for nil padding
    right_field_count = length(TableStructure.field_names(join_schema))
    
    # Determine left source indices (all previously joined sources)
    left_indices = joined_indices
    
    # Perform the join based on type
    case join.qual do
      :inner -> inner_join(left_results, join_objects, condition_evaluator, sources, left_indices, join_source_idx)
      :left -> left_join(left_results, join_objects, condition_evaluator, sources, left_indices, join_source_idx, right_field_count)
      :right -> right_join(left_results, join_objects, condition_evaluator, sources, left_indices, join_source_idx, right_field_count)
      :full -> full_join(left_results, join_objects, condition_evaluator, sources, left_indices, join_source_idx, right_field_count)
      :inner_lateral -> inner_join(left_results, join_objects, condition_evaluator, sources, left_indices, join_source_idx)
      :left_lateral -> left_join(left_results, join_objects, condition_evaluator, sources, left_indices, join_source_idx, right_field_count)
      _ -> inner_join(left_results, join_objects, condition_evaluator, sources, left_indices, join_source_idx)
    end
  end

  @spec find_source_index({atom(), module()}, %{non_neg_integer() => {atom(), module()}}) :: non_neg_integer() | nil
  defp find_source_index(source, sources) do
    Enum.find_value(sources, fn {idx, s} -> if s == source, do: idx end)
  end


  @spec filter_wheres_for_source([Ecto.Query.BooleanExpr.t()], non_neg_integer(), %{non_neg_integer() => {atom(), module()}}) :: [Ecto.Query.BooleanExpr.t()]
  defp filter_wheres_for_source(wheres, source_idx, _sources) do
    # Filter where clauses to only those that reference ONLY the given source
    # (not other sources)
    Enum.filter(wheres, fn %Ecto.Query.BooleanExpr{expr: expr} ->
      all_indices = extract_source_indices(expr)
      # Must reference this source and no other sources
      source_idx in all_indices and length(all_indices) == 1
    end)
  end

  @spec references_other_sources?(term(), non_neg_integer()) :: boolean()
  defp references_other_sources?(expr, source_idx) do
    all_indices = extract_source_indices(expr)
    other_indices = Enum.reject(all_indices, &(&1 == source_idx))
    length(other_indices) > 0
  end

  @spec references_source?(term(), non_neg_integer()) :: boolean()
  defp references_source?({:==, _, [left, right]}, source_idx) do
    references_source?(left, source_idx) or references_source?(right, source_idx)
  end

  defp references_source?({op, _, [left, right]}, source_idx) when op in [:and, :or, :!=, :<, :>, :<=, :>=] do
    references_source?(left, source_idx) or references_source?(right, source_idx)
  end

  defp references_source?({:not, _, [expr]}, source_idx) do
    references_source?(expr, source_idx)
  end

  defp references_source?({{:., _, [{:&, _, [idx]}, _]}, _, _}, source_idx) do
    idx == source_idx
  end

  defp references_source?({:., _, [{:&, _, [idx]}, _]}, source_idx) do
    idx == source_idx
  end

  defp references_source?(_, _), do: false

  @spec remap_source_indices([Ecto.Query.BooleanExpr.t()], non_neg_integer(), non_neg_integer()) :: [Ecto.Query.BooleanExpr.t()]
  defp remap_source_indices(wheres, old_idx, new_idx) do
    Enum.map(wheres, fn %Ecto.Query.BooleanExpr{expr: expr} = where ->
      %{where | expr: remap_expr_source_indices(expr, old_idx, new_idx)}
    end)
  end

  @spec remap_expr_source_indices(term(), non_neg_integer(), non_neg_integer()) :: term()
  defp remap_expr_source_indices({{:., _, [{:&, _, [source_idx]}, field_name]}, meta, args}, old_idx, new_idx) when source_idx == old_idx do
    {{:., meta, [{:&, [], [new_idx]}, field_name]}, meta, args}
  end

  defp remap_expr_source_indices({{:., _, [{:&, _, [source_idx]}, field_name]}, meta, args}, old_idx, new_idx) do
    # Keep other source indices unchanged
    {{:., meta, [{:&, [], [source_idx]}, field_name]}, meta, args}
  end

  defp remap_expr_source_indices({:., meta, [{:&, _, [source_idx]}, field_name]}, old_idx, new_idx) when source_idx == old_idx do
    {:., meta, [{:&, [], [new_idx]}, field_name]}
  end

  defp remap_expr_source_indices({:., meta, [{:&, _, [source_idx]}, field_name]}, old_idx, new_idx) do
    # Keep other source indices unchanged
    {:., meta, [{:&, [], [source_idx]}, field_name]}
  end

  defp remap_expr_source_indices({op, meta, [left, right]}, old_idx, new_idx) when op in [:==, :!=, :<, :>, :<=, :>=, :and, :or] do
    {op, meta, [remap_expr_source_indices(left, old_idx, new_idx), remap_expr_source_indices(right, old_idx, new_idx)]}
  end

  defp remap_expr_source_indices({:not, meta, [expr]}, old_idx, new_idx) do
    {:not, meta, [remap_expr_source_indices(expr, old_idx, new_idx)]}
  end

  defp remap_expr_source_indices(expr, _, _), do: expr

  @spec build_condition_evaluator(
    Ecto.Query.BooleanExpr.t() | Ecto.Query.QueryExpr.t(),
    map(),
    keyword()
  ) :: (tuple(), tuple(), [non_neg_integer()], non_neg_integer() -> boolean())
  defp build_condition_evaluator(%Ecto.Query.BooleanExpr{expr: expr}, sources, params) do
    # Build join order for field extraction
    # For left tuple (already joined sources), use only left_indices
    # For right tuple (being joined), use the full join order including right_source_idx
    fn left_tuple, right_tuple, left_indices, right_source_idx ->
      # Full join order includes the right source (for future reference)
      full_join_order = left_indices ++ [right_source_idx]
      # But when extracting from left_tuple, we only use left_indices since right isn't joined yet
      left_join_order = left_indices
      evaluate_condition(expr, left_tuple, right_tuple, left_indices, right_source_idx, sources, params, left_join_order, full_join_order)
    end
  end

  defp build_condition_evaluator(%Ecto.Query.QueryExpr{expr: expr}, sources, params) do
    # QueryExpr has the same structure - extract expr and treat as BooleanExpr
    build_condition_evaluator(%Ecto.Query.BooleanExpr{expr: expr}, sources, params)
  end

  defp build_condition_evaluator(_, _, _), do: fn _, _, _, _ -> true end

  @spec evaluate_condition(term(), tuple(), tuple() | list(), [non_neg_integer()], non_neg_integer(), %{non_neg_integer() => {atom(), module()}}, keyword(), [non_neg_integer()], [non_neg_integer()]) :: boolean() | term()
  defp evaluate_condition({:==, _, [left, right]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, _full_join_order) do
    left_val = extract_field_value(left, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    right_val = extract_field_value(right, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    # Handle nil comparisons - if either side is nil, they're not equal unless both are nil
    case {left_val, right_val} do
      {nil, nil} -> true
      {nil, _} -> false
      {_, nil} -> false
      {l, r} -> l == r
    end
  end

  defp evaluate_condition({:!=, _, [left, right]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, _full_join_order) do
    left_val = extract_field_value(left, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    right_val = extract_field_value(right, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    left_val != right_val
  end

  defp evaluate_condition({:<, _, [left, right]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, _full_join_order) do
    left_val = extract_field_value(left, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    right_val = extract_field_value(right, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    left_val < right_val
  end

  defp evaluate_condition({:>, _, [left, right]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, _full_join_order) do
    left_val = extract_field_value(left, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    right_val = extract_field_value(right, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    left_val > right_val
  end

  defp evaluate_condition({:<=, _, [left, right]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, _full_join_order) do
    left_val = extract_field_value(left, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    right_val = extract_field_value(right, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    left_val <= right_val
  end

  defp evaluate_condition({:>=, _, [left, right]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, _full_join_order) do
    left_val = extract_field_value(left, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    right_val = extract_field_value(right, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    left_val >= right_val
  end

  defp evaluate_condition({:and, _, [left, right]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, full_join_order) do
    evaluate_condition(left, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, full_join_order) and
      evaluate_condition(right, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, full_join_order)
  end

  defp evaluate_condition({:or, _, [left, right]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, full_join_order) do
    evaluate_condition(left, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, full_join_order) or
      evaluate_condition(right, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, full_join_order)
  end

  defp evaluate_condition({:not, _, [expr]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, full_join_order) do
    not evaluate_condition(expr, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, full_join_order)
  end

  defp evaluate_condition(value, _, _, _, _, _, _, _, _) when not is_tuple(value), do: value

  @spec extract_field_value(term(), tuple(), tuple() | list(), [non_neg_integer()], non_neg_integer(), %{non_neg_integer() => {atom(), module()}}, keyword(), [non_neg_integer()]) :: term()
  defp extract_field_value({{:., _, [{:&, _, [source_idx]}, field_name]}, _, _}, left_tuple, right_tuple, left_indices, right_idx, sources, _params, join_order) do
    extract_field_from_tuple(source_idx, field_name, left_tuple, right_tuple, left_indices, right_idx, sources, join_order)
  end

  defp extract_field_value({:., meta, [{:&, _, [source_idx]}, field_name]}, left_tuple, right_tuple, left_indices, right_idx, sources, _params, join_order) do
    extract_field_from_tuple(source_idx, field_name, left_tuple, right_tuple, left_indices, right_idx, sources, join_order)
  end
  
  # Handle field access with different metadata structures
  defp extract_field_value({{:., meta, [{:&, _, [source_idx]}, field_name]}, _, args}, left_tuple, right_tuple, left_indices, right_idx, sources, _params, join_order) when is_list(args) do
    extract_field_from_tuple(source_idx, field_name, left_tuple, right_tuple, left_indices, right_idx, sources, join_order)
  end

  defp extract_field_value({:^, _, [param_idx]}, _, _, _, _, _, params, _join_order) do
    # Parameter reference in join conditions
    Enum.at(params, param_idx)
  end

  # Catch-all: try to recursively search for field access in AST structures
  defp extract_field_value({_, _, args} = expr, left_tuple, right_tuple, left_indices, right_idx, sources, params, join_order) when is_tuple(expr) and tuple_size(expr) == 3 do
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

  defp extract_field_value(value, _, _, _, _, _, _, _), do: value

  @spec extract_field_from_tuple(non_neg_integer(), atom(), tuple(), tuple() | list(), [non_neg_integer()], non_neg_integer(), %{non_neg_integer() => {atom(), module()}}, [non_neg_integer()]) :: term()
  defp extract_field_from_tuple(source_idx, field_name, left_tuple, right_tuple, left_indices, right_idx, sources, join_order) do
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
  defp extract_field_from_joined_tuple(tuple, field_name, {_, schema}) when is_tuple(tuple) do
    field_names = TableStructure.field_names(schema)
    field_index = Enum.find_index(field_names, &(&1 == field_name))
    # Elixir tuples are 0-indexed
    if field_index != nil, do: elem(tuple, field_index), else: nil
  end

  defp extract_field_from_joined_tuple(list, field_name, {_, schema}) when is_list(list) do
    field_names = TableStructure.field_names(schema)
    field_index = Enum.find_index(field_names, &(&1 == field_name))
    if field_index != nil, do: Enum.at(list, field_index), else: nil
  end

  defp extract_field_from_joined_tuple(_, _, _), do: nil

  @spec extract_field_from_combined_tuple(tuple() | list(), atom(), non_neg_integer(), %{non_neg_integer() => {atom(), module()}}, [non_neg_integer()]) :: term()
  defp extract_field_from_combined_tuple(combined_tuple, field_name, source_idx, sources, join_order) when is_tuple(combined_tuple) do
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
      elem(combined_tuple, tuple_pos)
    else
      nil
    end
  end

  defp extract_field_from_combined_tuple(combined_list, field_name, source_idx, sources, join_order) when is_list(combined_list) do
    # Handle lists (after structure_join_results conversion)
    offset = calculate_field_offset(source_idx, sources, join_order)
    {_, schema} = sources[source_idx]
    field_names = TableStructure.field_names(schema)
    field_index = Enum.find_index(field_names, &(&1 == field_name))
    
    # Lists are 0-indexed
    if field_index, do: Enum.at(combined_list, offset + field_index), else: nil
  end

  defp extract_field_from_combined_tuple(_, _, _, _, _), do: nil

  @spec calculate_field_offset(non_neg_integer(), %{non_neg_integer() => {atom(), module()}}, [non_neg_integer()]) :: non_neg_integer()
  defp calculate_field_offset(target_idx, sources, join_order) do
    # Sum field counts of all sources that come before target_idx in join_order
    # join_order is the order in which sources appear in the combined tuple: [0, 1, 2, ...]
    join_order
    |> Enum.take_while(&(&1 != target_idx))
    |> Enum.reduce(0, fn idx, acc ->
      acc + get_field_count_for_source(sources, idx)
    end)
  end

  @spec inner_join([tuple() | list()], [tuple() | list()], (tuple(), tuple() | list(), [non_neg_integer()], non_neg_integer() -> boolean()), %{non_neg_integer() => {atom(), module()}}, [non_neg_integer()], non_neg_integer()) :: [tuple()]
  defp inner_join(left_results, right_results, condition_evaluator, sources, left_indices, right_idx) do
    Enum.flat_map(left_results, fn left_tuple ->
      matching_right = Enum.filter(right_results, fn right_tuple ->
        condition_evaluator.(left_tuple, right_tuple, left_indices, right_idx)
      end)
      
      Enum.map(matching_right, fn right_tuple ->
        combine_tuples(left_tuple, right_tuple)
      end)
    end)
  end

  @spec left_join([tuple() | list()], [tuple() | list()], (tuple(), tuple() | list(), [non_neg_integer()], non_neg_integer() -> boolean()), %{non_neg_integer() => {atom(), module()}}, [non_neg_integer()], non_neg_integer(), non_neg_integer()) :: [tuple()]
  defp left_join(left_results, right_results, condition_evaluator, sources, left_indices, right_idx, right_field_count) do
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

  @spec right_join([tuple() | list()], [tuple() | list()], (tuple(), tuple() | list(), [non_neg_integer()], non_neg_integer() -> boolean()), %{non_neg_integer() => {atom(), module()}}, [non_neg_integer()], non_neg_integer(), non_neg_integer()) :: [tuple()]
  defp right_join(left_results, right_results, condition_evaluator, sources, left_indices, right_idx, _right_field_count) do
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

  @spec full_join([tuple() | list()], [tuple() | list()], (tuple(), tuple() | list(), [non_neg_integer()], non_neg_integer() -> boolean()), %{non_neg_integer() => {atom(), module()}}, [non_neg_integer()], non_neg_integer(), non_neg_integer()) :: [tuple()]
  defp full_join(left_results, right_results, condition_evaluator, sources, left_indices, right_idx, right_field_count) do
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

  @spec get_field_count_for_source(%{non_neg_integer() => {atom(), module()}}, non_neg_integer()) :: non_neg_integer()
  defp get_field_count_for_source(sources, source_idx) do
    case sources[source_idx] do
      {_, schema} -> length(TableStructure.field_names(schema))
      _ -> 0
    end
  end

  @spec create_nil_tuple(non_neg_integer()) :: tuple()
  defp create_nil_tuple(field_count) do
    List.to_tuple(List.duplicate(nil, field_count))
  end

  @spec combine_tuples(tuple() | list(), tuple() | list()) :: tuple()
  defp combine_tuples(left_tuple, right_tuple) when is_tuple(left_tuple) and is_tuple(right_tuple) do
    left_list = Tuple.to_list(left_tuple)
    right_list = Tuple.to_list(right_tuple)
    List.to_tuple(left_list ++ right_list)
  end

  defp combine_tuples(left_list, right_list) when is_list(left_list) and is_list(right_list) do
    # Handle case where ETS returns lists instead of tuples
    List.to_tuple(left_list ++ right_list)
  end

  defp combine_tuples(left, right) do
    # Convert to lists if needed, then combine
    left_list = if is_tuple(left), do: Tuple.to_list(left), else: left
    right_list = if is_tuple(right), do: Tuple.to_list(right), else: right
    List.to_tuple(left_list ++ right_list)
  end

  @spec apply_all_wheres([tuple() | list()], [Ecto.Query.BooleanExpr.t()], %{non_neg_integer() => {atom(), module()}}, keyword(), [non_neg_integer()]) :: [tuple() | list()]
  defp apply_all_wheres(results, wheres, sources, params, join_order) do
    # Apply ALL where clauses on the joined results
    # This is necessary because match specs may not always filter correctly,
    # and we need to ensure all where conditions are applied
    if Enum.empty?(wheres) do
      results
    else
      # Build evaluators for all where conditions
      evaluators = Enum.map(wheres, fn %Ecto.Query.BooleanExpr{expr: expr} ->
        build_where_evaluator(expr, sources, params, join_order)
      end)
      
      # Filter results using all evaluators (AND logic)
      Enum.filter(results, fn tuple ->
        # Convert tuple to list if needed for field extraction
        tuple_for_eval = if is_tuple(tuple), do: tuple, else: List.to_tuple(tuple)
        Enum.all?(evaluators, fn evaluator -> evaluator.(tuple_for_eval, sources) end)
      end)
    end
  end

  @spec apply_post_join_wheres([tuple() | list()], [Ecto.Query.BooleanExpr.t()], %{non_neg_integer() => {atom(), module()}}, keyword()) :: [tuple() | list()]
  defp apply_post_join_wheres(results, wheres, sources, params) do
    # This function is kept for backwards compatibility but is no longer used
    # All wheres are now applied via apply_all_wheres
    results
  end

  @spec build_where_evaluator(term(), %{non_neg_integer() => {atom(), module()}}, keyword(), [non_neg_integer()]) :: (tuple(), %{non_neg_integer() => {atom(), module()}} -> boolean())
  defp build_where_evaluator(expr, _sources, params, join_order) do
    fn tuple, sources ->
      evaluate_where_condition(expr, tuple, sources, params, join_order)
    end
  end

  @spec evaluate_where_condition(term(), tuple(), %{non_neg_integer() => {atom(), module()}}, keyword(), [non_neg_integer()]) :: boolean() | term()
  defp evaluate_where_condition({:==, _, [left, right]}, tuple, sources, params, join_order) do
    left_val = extract_where_field_value(left, tuple, sources, params, join_order)
    right_val = extract_where_field_value(right, tuple, sources, params, join_order)
    # Handle nil comparisons - if either side is nil, they're not equal unless both are nil
    case {left_val, right_val} do
      {nil, nil} -> true
      {nil, _} -> false
      {_, nil} -> false
      {l, r} -> l == r
    end
  end

  defp evaluate_where_condition({:!=, _, [left, right]}, tuple, sources, params, join_order) do
    left_val = extract_where_field_value(left, tuple, sources, params, join_order)
    right_val = extract_where_field_value(right, tuple, sources, params, join_order)
    left_val != right_val
  end

  defp evaluate_where_condition({:<, _, [left, right]}, tuple, sources, params, join_order) do
    left_val = extract_where_field_value(left, tuple, sources, params, join_order)
    right_val = extract_where_field_value(right, tuple, sources, params, join_order)
    left_val < right_val
  end

  defp evaluate_where_condition({:>, _, [left, right]}, tuple, sources, params, join_order) do
    left_val = extract_where_field_value(left, tuple, sources, params, join_order)
    right_val = extract_where_field_value(right, tuple, sources, params, join_order)
    left_val > right_val
  end

  defp evaluate_where_condition({:<=, _, [left, right]}, tuple, sources, params, join_order) do
    left_val = extract_where_field_value(left, tuple, sources, params, join_order)
    right_val = extract_where_field_value(right, tuple, sources, params, join_order)
    left_val <= right_val
  end

  defp evaluate_where_condition({:>=, _, [left, right]}, tuple, sources, params, join_order) do
    left_val = extract_where_field_value(left, tuple, sources, params, join_order)
    right_val = extract_where_field_value(right, tuple, sources, params, join_order)
    left_val >= right_val
  end

  defp evaluate_where_condition({:and, _, [left, right]}, tuple, sources, params, join_order) do
    evaluate_where_condition(left, tuple, sources, params, join_order) and
      evaluate_where_condition(right, tuple, sources, params, join_order)
  end

  defp evaluate_where_condition({:or, _, [left, right]}, tuple, sources, params, join_order) do
    evaluate_where_condition(left, tuple, sources, params, join_order) or
      evaluate_where_condition(right, tuple, sources, params, join_order)
  end

  defp evaluate_where_condition({:not, _, [expr]}, tuple, sources, params, join_order) do
    not evaluate_where_condition(expr, tuple, sources, params, join_order)
  end

  defp evaluate_where_condition({:in, _, [field, values]}, tuple, sources, params, join_order) do
    field_val = extract_where_field_value(field, tuple, sources, params, join_order)
    value_list = extract_where_field_value(values, tuple, sources, params, join_order)
    field_val in value_list
  end

  defp evaluate_where_condition({:is_nil, _, [field]}, tuple, sources, params, join_order) do
    field_val = extract_where_field_value(field, tuple, sources, params, join_order)
    is_nil(field_val)
  end

  defp evaluate_where_condition(value, _, _, _, _) when not is_tuple(value), do: value

  @spec extract_where_field_value(term(), tuple(), %{non_neg_integer() => {atom(), module()}}, keyword(), [non_neg_integer()]) :: term()
  defp extract_where_field_value({{:., _, [{:&, _, [source_idx]}, field_name]}, _, _}, tuple, sources, _params, join_order) do
    extract_field_from_combined_tuple(tuple, field_name, source_idx, sources, join_order)
  end

  defp extract_where_field_value({:., meta, [{:&, _, [source_idx]}, field_name]}, tuple, sources, _params, join_order) do
    extract_field_from_combined_tuple(tuple, field_name, source_idx, sources, join_order)
  end
  
  # Handle field access with different metadata structures
  defp extract_where_field_value({{:., meta, [{:&, _, [source_idx]}, field_name]}, _, args}, tuple, sources, _params, join_order) when is_list(args) do
    extract_field_from_combined_tuple(tuple, field_name, source_idx, sources, join_order)
  end

  defp extract_where_field_value({:^, _, [param_idx]}, _, _, params, _join_order) do
    Enum.at(params, param_idx)
  end

  defp extract_where_field_value(value, _, _, _, _), do: value

  @spec references_multiple_sources?(term(), %{non_neg_integer() => {atom(), module()}}) :: boolean()
  defp references_multiple_sources?(expr, _sources) do
    source_indices = extract_source_indices(expr)
    length(Enum.uniq(source_indices)) > 1
  end

  @spec extract_source_indices(term()) :: [non_neg_integer()]
  defp extract_source_indices({{:., _, [{:&, _, [idx]}, _]}, _, _}) do
    [idx]
  end

  defp extract_source_indices({:., _, [{:&, _, [idx]}, _]}) do
    [idx]
  end
  
  # Handle field access with different metadata structures
  defp extract_source_indices({{:., meta, [{:&, _, [idx]}, field_name]}, _, args}) when is_list(args) do
    [idx]
  end
  
  defp extract_source_indices({:., meta, [{:&, _, [idx]}, field_name]}) do
    [idx]
  end

  defp extract_source_indices({op, _, [left, right]}) when op in [:==, :!=, :<, :>, :<=, :>=, :and, :or] do
    extract_source_indices(left) ++ extract_source_indices(right)
  end

  defp extract_source_indices({:not, _, [expr]}) do
    extract_source_indices(expr)
  end

  defp extract_source_indices({:^, _, _}), do: []
  
  # Catch-all: try to recursively search for source indices in any tuple structure
  # Only do this for tuples that look like AST nodes (have 3 elements)
  defp extract_source_indices({_, _, args} = expr) when is_tuple(expr) and tuple_size(expr) == 3 do
    # This looks like an AST node - recursively search args
    if is_list(args) do
      Enum.flat_map(args, &extract_source_indices/1)
    else
      extract_source_indices(args)
    end
  end
  
  defp extract_source_indices(expr) when is_list(expr) do
    Enum.flat_map(expr, &extract_source_indices/1)
  end
  
  defp extract_source_indices(_), do: []

  @spec map_results_to_select([tuple() | list()], Ecto.Query.t(), %{non_neg_integer() => {atom(), module()}}) :: [tuple() | list()]
  defp map_results_to_select(results, query, sources) do
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
    extract_field_from_combined_tuple(tuple, field_name, source_idx, sources, all_source_indices)
  end

  defp extract_select_field_value({:., _, [{:&, _, [source_idx]}, field_name]}, tuple, sources, all_source_indices) do
    extract_field_from_combined_tuple(tuple, field_name, source_idx, sources, all_source_indices)
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

  @spec extract_limit_value(term() | nil, keyword()) :: non_neg_integer() | nil
  defp extract_limit_value(nil, _params), do: nil
  
  # Handle LimitExpr struct (Ecto 3.11+)
  # LimitExpr has an `expr` field, not `count`
  defp extract_limit_value(%{__struct__: struct_name, expr: expr}, params) when struct_name in [Ecto.Query.LimitExpr, "Elixir.Ecto.Query.LimitExpr"] do
    extract_limit_expr(expr, params) || find_integer_in_ast(expr)
  end
  
  # Fallback for older LimitExpr structure with `count` field
  defp extract_limit_value(%{__struct__: struct_name, count: count}, params) when struct_name in [Ecto.Query.LimitExpr, "Elixir.Ecto.Query.LimitExpr"] do
    extract_limit_expr(count, params) || find_integer_in_ast(count)
  end
  
  defp extract_limit_value(%Ecto.Query.QueryExpr{expr: expr}, params) do
    # Try to extract from the expression using all methods
    extract_limit_expr(expr, params) || find_integer_in_ast(expr)
  end
  
  # Handle direct integer limit
  defp extract_limit_value(value, _params) when is_integer(value) do
    value
  end
  
  defp extract_limit_value(limit, params) do
    # Last resort: try to find any integer in the structure
    # First try extract_limit_expr in case it's a nested structure
    case extract_limit_expr(limit, params) do
      nil -> find_integer_in_ast(limit)
      value -> value
    end
  end
  
  
  @spec extract_limit_expr(term(), keyword()) :: non_neg_integer() | nil
  defp extract_limit_expr({:^, _, [param_idx]}, params) do
    Enum.at(params, param_idx)
  end
  
  defp extract_limit_expr(value, _params) when is_integer(value) and value > 0 do
    value
  end
  
  defp extract_limit_expr(value, _params) when is_integer(value) do
    nil  # Ignore negative or zero limits
  end
  
  # Handle AST forms like {:., [], [5]} or similar
  defp extract_limit_expr({:., _, [value]}, params) when is_integer(value) do
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
  defp extract_limit_expr({:limit, _, [value]}, params) when is_integer(value) do
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

  @spec structure_join_results([tuple() | list()], Ecto.Query.t(), %{non_neg_integer() => {atom(), module()}}) :: [tuple() | list()]
  defp structure_join_results(results, query, _sources) do
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
end
