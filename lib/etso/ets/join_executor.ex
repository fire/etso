defmodule Etso.ETS.JoinExecutor do
  @moduledoc """
  The Join Executor module is responsible for executing join queries in-memory.
  It supports inner, left, right, and full outer joins with complex conditions.
  """

  alias Etso.Adapter.TableRegistry
  alias Etso.ETS.Join.ConditionEvaluator
  alias Etso.ETS.Join.Operations
  alias Etso.ETS.Join.ResultMapper
  alias Etso.ETS.Join.WhereEvaluator
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
    filtered_results = WhereEvaluator.apply_all(results, query.wheres, sources, params, join_order)
    
    # Map results according to select clause if present (before sorting)
    # We need to map while we still have tuples for sorting
    mapped_results = if query.select do
      ResultMapper.map_to_select(filtered_results, query, sources)
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
    structured_results = ResultMapper.structure(sorted_results, query, sources)
    
    # Apply limit if present
    limit_value = ResultMapper.extract_limit(query.limit, params)
    
    limited_results = case limit_value do
      value when is_integer(value) and value >= 0 ->
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
    condition_evaluator = ConditionEvaluator.build(%Ecto.Query.BooleanExpr{expr: condition_expr}, sources, params)
    
    # Get field counts for nil padding
    right_field_count = length(TableStructure.field_names(join_schema))
    
    # Determine left source indices (all previously joined sources)
    left_indices = joined_indices
    
    # Perform the join based on type
    case join.qual do
      :inner -> Operations.inner_join(left_results, join_objects, condition_evaluator, sources, left_indices, join_source_idx)
      :left -> Operations.left_join(left_results, join_objects, condition_evaluator, sources, left_indices, join_source_idx, right_field_count)
      :right -> Operations.right_join(left_results, join_objects, condition_evaluator, sources, left_indices, join_source_idx, right_field_count)
      :full -> Operations.full_join(left_results, join_objects, condition_evaluator, sources, left_indices, join_source_idx, right_field_count)
      :inner_lateral -> Operations.inner_join(left_results, join_objects, condition_evaluator, sources, left_indices, join_source_idx)
      :left_lateral -> Operations.left_join(left_results, join_objects, condition_evaluator, sources, left_indices, join_source_idx, right_field_count)
      _ -> Operations.inner_join(left_results, join_objects, condition_evaluator, sources, left_indices, join_source_idx)
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

  defp remap_expr_source_indices({{:., _, [{:&, _, [source_idx]}, field_name]}, meta, args}, _old_idx, _new_idx) do
    # Keep other source indices unchanged
    {{:., meta, [{:&, [], [source_idx]}, field_name]}, meta, args}
  end

  defp remap_expr_source_indices({:., meta, [{:&, _, [source_idx]}, field_name]}, old_idx, new_idx) when source_idx == old_idx do
    {:., meta, [{:&, [], [new_idx]}, field_name]}
  end

  defp remap_expr_source_indices({:., meta, [{:&, _, [source_idx]}, field_name]}, _old_idx, _new_idx) do
    # Keep other source indices unchanged
    {:., meta, [{:&, [], [source_idx]}, field_name]}
  end

  defp remap_expr_source_indices({op, meta, [left, right]}, old_idx, new_idx) when op in [:==, :!=, :<, :>, :<=, :>=, :and, :or, :in] do
    {op, meta, [remap_expr_source_indices(left, old_idx, new_idx), remap_expr_source_indices(right, old_idx, new_idx)]}
  end

  defp remap_expr_source_indices({:not, meta, [expr]}, old_idx, new_idx) do
    {:not, meta, [remap_expr_source_indices(expr, old_idx, new_idx)]}
  end

  defp remap_expr_source_indices(expr, _, _), do: expr

  @spec extract_source_indices(term()) :: [non_neg_integer()]
  defp extract_source_indices({{:., _, [{:&, _, [idx]}, _]}, _, _}) do
    [idx]
  end

  defp extract_source_indices({:., _, [{:&, _, [idx]}, _]}) do
    [idx]
  end

  defp extract_source_indices({op, _, [left, right]}) when op in [:==, :!=, :<, :>, :<=, :>=, :and, :or, :in] do
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
end
