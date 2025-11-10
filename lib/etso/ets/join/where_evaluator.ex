defmodule Etso.ETS.Join.WhereEvaluator do
  @moduledoc """
  Handles evaluation of WHERE clauses in join contexts.
  """

  alias Etso.ETS.Join.FieldExtractor

  @doc """
  Applies all where clauses to joined results.
  """
  @spec apply_all([tuple() | list()], [Ecto.Query.BooleanExpr.t()], %{non_neg_integer() => {atom(), module()}}, keyword(), [non_neg_integer()]) :: [tuple() | list()]
  def apply_all(results, wheres, sources, params, join_order) do
    # Apply ALL where clauses on the joined results
    # This is necessary because match specs may not always filter correctly,
    # and we need to ensure all where conditions are applied
    if Enum.empty?(wheres) do
      results
    else
      # Build evaluators for all where conditions
      evaluators = Enum.map(wheres, fn %Ecto.Query.BooleanExpr{expr: expr} ->
        build_evaluator(expr, sources, params, join_order)
      end)
      
      # Filter results using all evaluators (AND logic)
      Enum.filter(results, fn tuple ->
        # Convert tuple to list if needed for field extraction
        tuple_for_eval = if is_tuple(tuple), do: tuple, else: List.to_tuple(tuple)
        Enum.all?(evaluators, fn evaluator -> evaluator.(tuple_for_eval, sources) end)
      end)
    end
  end

  @spec build_evaluator(term(), %{non_neg_integer() => {atom(), module()}}, keyword(), [non_neg_integer()]) :: (tuple(), %{non_neg_integer() => {atom(), module()}} -> boolean())
  defp build_evaluator(expr, _sources, params, join_order) do
    fn tuple, sources ->
      evaluate(expr, tuple, sources, params, join_order)
    end
  end

  @spec evaluate(term(), tuple(), %{non_neg_integer() => {atom(), module()}}, keyword(), [non_neg_integer()]) :: boolean() | term()
  def evaluate({:==, _, [left, right]}, tuple, sources, params, join_order) do
    left_val = extract_field_value(left, tuple, sources, params, join_order)
    right_val = extract_field_value(right, tuple, sources, params, join_order)
    # Handle nil comparisons - if either side is nil, they're not equal unless both are nil
    case {left_val, right_val} do
      {nil, nil} -> true
      {nil, _} -> false
      {_, nil} -> false
      {l, r} -> l == r
    end
  end

  def evaluate({:!=, _, [left, right]}, tuple, sources, params, join_order) do
    left_val = extract_field_value(left, tuple, sources, params, join_order)
    right_val = extract_field_value(right, tuple, sources, params, join_order)
    left_val != right_val
  end

  def evaluate({:<, _, [left, right]}, tuple, sources, params, join_order) do
    left_val = extract_field_value(left, tuple, sources, params, join_order)
    right_val = extract_field_value(right, tuple, sources, params, join_order)
    left_val < right_val
  end

  def evaluate({:>, _, [left, right]}, tuple, sources, params, join_order) do
    left_val = extract_field_value(left, tuple, sources, params, join_order)
    right_val = extract_field_value(right, tuple, sources, params, join_order)
    left_val > right_val
  end

  def evaluate({:<=, _, [left, right]}, tuple, sources, params, join_order) do
    left_val = extract_field_value(left, tuple, sources, params, join_order)
    right_val = extract_field_value(right, tuple, sources, params, join_order)
    left_val <= right_val
  end

  def evaluate({:>=, _, [left, right]}, tuple, sources, params, join_order) do
    left_val = extract_field_value(left, tuple, sources, params, join_order)
    right_val = extract_field_value(right, tuple, sources, params, join_order)
    left_val >= right_val
  end

  def evaluate({:and, _, [left, right]}, tuple, sources, params, join_order) do
    evaluate(left, tuple, sources, params, join_order) and
      evaluate(right, tuple, sources, params, join_order)
  end

  def evaluate({:or, _, [left, right]}, tuple, sources, params, join_order) do
    evaluate(left, tuple, sources, params, join_order) or
      evaluate(right, tuple, sources, params, join_order)
  end

  def evaluate({:not, _, [expr]}, tuple, sources, params, join_order) do
    not evaluate(expr, tuple, sources, params, join_order)
  end

  def evaluate({:in, _, [field, values]}, tuple, sources, params, join_order) do
    field_val = extract_field_value(field, tuple, sources, params, join_order)
    value_list = extract_field_value(values, tuple, sources, params, join_order)
    field_val in value_list
  end

  def evaluate({:is_nil, _, [field]}, tuple, sources, params, join_order) do
    field_val = extract_field_value(field, tuple, sources, params, join_order)
    is_nil(field_val)
  end

  def evaluate(value, _, _, _, _) when not is_tuple(value), do: value

  @spec extract_field_value(term(), tuple(), %{non_neg_integer() => {atom(), module()}}, keyword(), [non_neg_integer()]) :: term()
  defp extract_field_value({{:., _, [{:&, _, [source_idx]}, field_name]}, _, _}, tuple, sources, _params, join_order) do
    FieldExtractor.extract_field_from_combined_tuple(tuple, field_name, source_idx, sources, join_order)
  end

  defp extract_field_value({:., _meta, [{:&, _, [source_idx]}, field_name]}, tuple, sources, _params, join_order) do
    FieldExtractor.extract_field_from_combined_tuple(tuple, field_name, source_idx, sources, join_order)
  end

  defp extract_field_value({:^, _, [param_idx]}, _, _, params, _join_order) do
    Enum.at(params, param_idx)
  end

  defp extract_field_value(value, _, _, _, _), do: value
end

