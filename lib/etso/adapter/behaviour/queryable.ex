defmodule Etso.Adapter.Behaviour.Queryable do
  @moduledoc false
  @behaviour Ecto.Adapter.Queryable

  alias Etso.Adapter.TableRegistry
  alias Etso.ETS.AggregateExtractor
  alias Etso.ETS.JoinExecutor
  alias Etso.ETS.MatchSpecification
  alias Etso.ETS.ObjectsSorter

  defp get_repo(adapter_meta) do
    adapter_meta.dynamic_repo || adapter_meta.repo
  end

  @impl Ecto.Adapter.Queryable
  def prepare(:all, %Ecto.Query{joins: [_ | _]} = query) do
    # Check if this is a join query
    {:nocache, {:join, query}}
  end

  @impl Ecto.Adapter.Queryable
  def prepare(:all, %Ecto.Query{select: %Ecto.Query.SelectExpr{fields: fields}} = query) do
    # Check if this is an aggregate query
    case detect_aggregate(fields) do
      nil ->
        {:nocache, {:select, query}}
      aggregate_info ->
        {:nocache, {:aggregate, query, aggregate_info}}
    end
  end

  @impl Ecto.Adapter.Queryable
  def prepare(:all, %Ecto.Query{} = query) do
    {:nocache, {:select, query}}
  end

  @impl Ecto.Adapter.Queryable
  def prepare(:delete_all, %Ecto.Query{wheres: []} = query) do
    {:nocache, {:delete_all_objects, query}}
  end

  @impl Ecto.Adapter.Queryable
  def prepare(:delete_all, %Ecto.Query{wheres: _} = query) do
    {:nocache, {:match_delete, query}}
  end

  defp detect_aggregate([field_expr]) do
    case field_expr do
      {:count, _, []} ->
        {:count, nil, false}
      {:count, _, [field]} ->
        {:count, extract_field_from_ast(field), false}
      {:count, _, [field, :distinct]} ->
        {:count, extract_field_from_ast(field), true}
      {:sum, _, [field]} ->
        {:sum, extract_field_from_ast(field), false}
      {:avg, _, [field]} ->
        {:avg, extract_field_from_ast(field), false}
      {:min, _, [field]} ->
        {:min, extract_field_from_ast(field), false}
      {:max, _, [field]} ->
        {:max, extract_field_from_ast(field), false}
      {:distinct, _, [{:count, _, []}]} ->
        {:count, nil, true}
      {:distinct, _, [{:count, _, [field]}]} ->
        {:count, extract_field_from_ast(field), true}
      _ ->
        nil
    end
  end

  defp detect_aggregate(_) do
    nil
  end

  defp extract_field_from_ast({{:., _, [{:&, _, _}, field_name]}, _, _}) when is_atom(field_name) do
    field_name
  end

  defp extract_field_from_ast({:., _, [{:&, _, _}, field_name]}) when is_atom(field_name) do
    field_name
  end

  defp extract_field_from_ast(field) when is_atom(field) do
    field
  end

  defp extract_field_from_ast(_) do
    nil
  end

  @impl Ecto.Adapter.Queryable
  def execute(adapter_meta, _, {:nocache, {:join, query}}, params, _) do
    JoinExecutor.execute(adapter_meta, query, params)
  end

  @impl Ecto.Adapter.Queryable
  def execute(adapter_meta, _, {:nocache, {:select, query}}, params, _) do
    repo = get_repo(adapter_meta)
    {_, schema} = query.from.source
    {:ok, ets_table} = TableRegistry.get_table(repo, schema)
    ets_match = MatchSpecification.build(query, params)
    ets_objects = :ets.select(ets_table, [ets_match])
    ets_count = length(ets_objects)
    {ets_count, ObjectsSorter.sort(ets_objects, query)}
  end

  @impl Ecto.Adapter.Queryable
  def execute(adapter_meta, _, {:nocache, {:delete_all_objects, query}}, params, _) do
    repo = get_repo(adapter_meta)
    {_, schema} = query.from.source
    {:ok, ets_table} = TableRegistry.get_table(repo, schema)
    ets_match = MatchSpecification.build(query, params)
    ets_objects = query.select && ObjectsSorter.sort(:ets.select(ets_table, [ets_match]), query)
    ets_count = :ets.info(ets_table, :size)
    true = :ets.delete_all_objects(ets_table)
    {ets_count, ets_objects || nil}
  end

  @impl Ecto.Adapter.Queryable
  def execute(adapter_meta, _, {:nocache, {:match_delete, query}}, params, _) do
    repo = get_repo(adapter_meta)
    {_, schema} = query.from.source
    {:ok, ets_table} = TableRegistry.get_table(repo, schema)
    ets_match = MatchSpecification.build(query, params)
    ets_objects = query.select && ObjectsSorter.sort(:ets.select(ets_table, [ets_match]), query)
    {ets_match_head, ets_match_body, _} = ets_match
    ets_match = {ets_match_head, ets_match_body, [true]}
    ets_count = :ets.select_delete(ets_table, [ets_match])
    {ets_count, ets_objects || nil}
  end

  @impl Ecto.Adapter.Queryable
  def execute(adapter_meta, _, {:nocache, {:aggregate, query, {op, field_name, distinct}}}, params, _) do
    repo = get_repo(adapter_meta)
    {_, schema} = query.from.source
    {:ok, ets_table} = TableRegistry.get_table(repo, schema)
    
    # For aggregates, we need all data to compute the aggregate
    # The match spec will return all fields (handled in MatchSpecification.build_body)
    ets_match = MatchSpecification.build(query, params)
    ets_objects = :ets.select(ets_table, [ets_match])

    result = compute_aggregate(ets_objects, schema, op, field_name, distinct)
    
    # For aggregates, Ecto expects the result in a format matching the select
    # Return as a list with a single-element list to match select structure
    {1, [[result]]}
  end

  defp compute_aggregate(ets_objects, _schema, :count, nil, distinct) do
    if distinct do
      length(Enum.uniq(ets_objects))
    else
      length(ets_objects)
    end
  end

  defp compute_aggregate(ets_objects, schema, :count, field_name, distinct) do
    values = AggregateExtractor.extract_values(ets_objects, schema, field_name, distinct)
    length(values)
  end

  defp compute_aggregate(ets_objects, schema, :sum, field_name, _distinct) do
    values = AggregateExtractor.extract_values(ets_objects, schema, field_name, false)
    numeric_values = Enum.map(values, &AggregateExtractor.to_numeric/1)
    filtered = AggregateExtractor.filter_nils(numeric_values)

    case filtered do
      [] -> nil
      _ -> Enum.sum(filtered)
    end
  end

  defp compute_aggregate(ets_objects, schema, :avg, field_name, _distinct) do
    values = AggregateExtractor.extract_values(ets_objects, schema, field_name, false)
    numeric_values = Enum.map(values, &AggregateExtractor.to_numeric/1)
    filtered = AggregateExtractor.filter_nils(numeric_values)

    case filtered do
      [] -> nil
      filtered ->
        sum = Enum.sum(filtered)
        count = length(filtered)
        if count > 0, do: sum / count, else: nil
    end
  end

  defp compute_aggregate(ets_objects, schema, :min, field_name, _distinct) do
    values = AggregateExtractor.extract_values(ets_objects, schema, field_name, false)
    numeric_values = Enum.map(values, &AggregateExtractor.to_numeric/1)
    filtered = AggregateExtractor.filter_nils(numeric_values)

    case filtered do
      [] -> nil
      _ -> Enum.min(filtered)
    end
  end

  defp compute_aggregate(ets_objects, schema, :max, field_name, _distinct) do
    values = AggregateExtractor.extract_values(ets_objects, schema, field_name, false)
    numeric_values = Enum.map(values, &AggregateExtractor.to_numeric/1)
    filtered = AggregateExtractor.filter_nils(numeric_values)

    case filtered do
      [] -> nil
      _ -> Enum.max(filtered)
    end
  end

  @impl Ecto.Adapter.Queryable
  def stream(adapter_meta, _, {:nocache, {:select, query}}, params, options) do
    repo = get_repo(adapter_meta)
    {_, schema} = query.from.source
    {:ok, ets_table} = TableRegistry.get_table(repo, schema)
    ets_match = MatchSpecification.build(query, params)
    ets_limit = Keyword.get(options, :max_rows, 500)
    stream_start_fun = fn -> stream_start(ets_table, ets_match, ets_limit) end
    stream_next_fun = fn acc -> stream_next(acc) end
    stream_after_fun = fn acc -> stream_after(ets_table, acc) end
    Stream.resource(stream_start_fun, stream_next_fun, stream_after_fun)
  end

  defp stream_start(ets_table, ets_match, ets_limit) do
    :ets.safe_fixtable(ets_table, true)
    :ets.select(ets_table, [ets_match], ets_limit)
  end

  defp stream_next(:"$end_of_table") do
    {:halt, :ok}
  end

  defp stream_next({ets_objects, ets_continuation}) do
    {[{length(ets_objects), ets_objects}], :ets.select(ets_continuation)}
  end

  defp stream_after(ets_table, :ok) do
    :ets.safe_fixtable(ets_table, false)
  end

  defp stream_after(_, acc) do
    acc
  end
end
