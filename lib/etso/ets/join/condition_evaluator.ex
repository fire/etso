defmodule Etso.ETS.Join.ConditionEvaluator do
  @moduledoc """
  Handles evaluation of join conditions (ON clauses).
  """

  alias Etso.ETS.Join.FieldExtractor

  @doc """
  Builds a condition evaluator function for a join condition.
  """
  @spec build(
    Ecto.Query.BooleanExpr.t() | Ecto.Query.QueryExpr.t(),
    map(),
    keyword()
  ) :: (tuple(), tuple(), [non_neg_integer()], non_neg_integer() -> boolean())
  def build(%Ecto.Query.BooleanExpr{expr: expr}, sources, params) do
    # Build join order for field extraction
    # For left tuple (already joined sources), use only left_indices
    # For right tuple (being joined), use the full join order including right_source_idx
    fn left_tuple, right_tuple, left_indices, right_source_idx ->
      # Full join order includes the right source (for future reference)
      full_join_order = left_indices ++ [right_source_idx]
      # But when extracting from left_tuple, we only use left_indices since right isn't joined yet
      left_join_order = left_indices
      evaluate(expr, left_tuple, right_tuple, left_indices, right_source_idx, sources, params, left_join_order, full_join_order)
    end
  end

  def build(%Ecto.Query.QueryExpr{expr: expr}, sources, params) do
    # QueryExpr has the same structure - extract expr and treat as BooleanExpr
    build(%Ecto.Query.BooleanExpr{expr: expr}, sources, params)
  end

  def build(_, _, _), do: fn _, _, _, _ -> true end

  @spec evaluate(term(), tuple(), tuple() | list(), [non_neg_integer()], non_neg_integer(), %{non_neg_integer() => {atom(), module()}}, keyword(), [non_neg_integer()], [non_neg_integer()]) :: boolean() | term()
  def evaluate({:==, _, [left, right]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, _full_join_order) do
    left_val = FieldExtractor.extract_field_value(left, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    right_val = FieldExtractor.extract_field_value(right, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    # Handle nil comparisons - if either side is nil, they're not equal unless both are nil
    case {left_val, right_val} do
      {nil, nil} -> true
      {nil, _} -> false
      {_, nil} -> false
      {l, r} -> l == r
    end
  end

  def evaluate({:!=, _, [left, right]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, _full_join_order) do
    left_val = FieldExtractor.extract_field_value(left, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    right_val = FieldExtractor.extract_field_value(right, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    left_val != right_val
  end

  def evaluate({:<, _, [left, right]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, _full_join_order) do
    left_val = FieldExtractor.extract_field_value(left, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    right_val = FieldExtractor.extract_field_value(right, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    left_val < right_val
  end

  def evaluate({:>, _, [left, right]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, _full_join_order) do
    left_val = FieldExtractor.extract_field_value(left, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    right_val = FieldExtractor.extract_field_value(right, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    left_val > right_val
  end

  def evaluate({:<=, _, [left, right]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, _full_join_order) do
    left_val = FieldExtractor.extract_field_value(left, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    right_val = FieldExtractor.extract_field_value(right, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    left_val <= right_val
  end

  def evaluate({:>=, _, [left, right]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, _full_join_order) do
    left_val = FieldExtractor.extract_field_value(left, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    right_val = FieldExtractor.extract_field_value(right, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    left_val >= right_val
  end

  def evaluate({:and, _, [left, right]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, full_join_order) do
    evaluate(left, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, full_join_order) and
      evaluate(right, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, full_join_order)
  end

  def evaluate({:or, _, [left, right]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, full_join_order) do
    evaluate(left, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, full_join_order) or
      evaluate(right, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, full_join_order)
  end

  def evaluate({:not, _, [expr]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, full_join_order) do
    not evaluate(expr, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, full_join_order)
  end

  def evaluate({:is_nil, _, [field]}, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order, _full_join_order) do
    field_val = FieldExtractor.extract_field_value(field, left_tuple, right_tuple, left_indices, right_idx, sources, params, left_join_order)
    is_nil(field_val)
  end

  def evaluate(value, _, _, _, _, _, _, _, _) when not is_tuple(value), do: value
end

