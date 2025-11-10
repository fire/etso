defmodule Etso.ETS.ObjectsSorter do
  @moduledoc """
  The ETS Objects Sorter module is responsible for sorting results returned from ETS according to
  the sort predicates provided in the query.
  """

  alias Etso.ETS.TableStructure

  def sort(ets_objects, %Ecto.Query{order_bys: []}) do
    ets_objects
  end

  def sort(ets_objects, %Ecto.Query{} = query) do
    sort_predicates = build_sort_predicates(query)
    # Filter out predicates with nil indices - they can't be used for sorting
    valid_predicates = Enum.filter(sort_predicates, fn {_dir, idx} -> is_integer(idx) end)
    if valid_predicates == [] do
      ets_objects
    else
      Enum.sort(ets_objects, &compare(&1, &2, valid_predicates))
    end
  end

  defp build_sort_predicates(%Ecto.Query{} = query) do
    Enum.flat_map(query.order_bys, fn order_by ->
      expr = case order_by do
        %Ecto.Query.QueryExpr{expr: expr} -> expr
        %Ecto.Query.ByExpr{expr: expr} -> expr
      end
      Enum.map(expr, fn
        {direction, field} when is_atom(direction) and is_tuple(field) ->
          # Handle keyword list format: [asc: ast_expr]
          field_name = extract_field_name(field)
          index = find_field_index(query, field_name)
          {direction, index}
        {direction, field} ->
          # Handle tuple format: {:asc, field}
          field_name = extract_field_name(field)
          index = find_field_index(query, field_name)
          {direction, index}
      end)
    end)
  end

  defp find_field_index(query, field_name) when is_atom(field_name) do
    if query.select && query.select.fields do
      Enum.find_index(query.select.fields, fn field ->
        extracted = extract_field_name(field)
        extracted == field_name
      end)
    else
      # When there's no explicit select, fields are in schema order with primary key first
      # We need to get the schema and find the field index using TableStructure.field_names
      case query.from do
        {_source, schema} when is_atom(schema) ->
          field_names = TableStructure.field_names(schema)
          Enum.find_index(field_names, &(&1 == field_name))
        _ ->
          nil
      end
    end
  end

  defp find_field_index(_query, _field_name) do
    nil
  end

  defp extract_field_name({{:., _, [{:&, _, _}, field_name]}, _, _}) when is_atom(field_name) do
    field_name
  end

  defp extract_field_name({:., _, [{:&, _, _}, field_name]}) when is_atom(field_name) do
    field_name
  end

  defp extract_field_name(field) when is_atom(field) do
    field
  end

  defp extract_field_name(_) do
    nil
  end

  defp compare(lhs, rhs, [{direction, index} | predicates]) when is_integer(index) do
    lhs_val = Enum.at(lhs, index)
    rhs_val = Enum.at(rhs, index)
    case {direction, lhs_val, rhs_val} do
      {_, val, val} -> compare(lhs, rhs, predicates)
      {:asc, l, r} -> l < r
      {:desc, l, r} -> l > r
    end
  end

  defp compare(lhs, rhs, [{_direction, nil} | predicates]) do
    # Skip sorting by field if index is nil (field not found in select)
    compare(lhs, rhs, predicates)
  end

  defp compare(_lhs, _rhs, []) do
    true
  end
end
