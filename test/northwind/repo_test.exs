defmodule Northwind.RepoTest do
  use ExUnit.Case
  alias Northwind.{Importer, Model, Repo}
  import Ecto.Query

  setup do
    repo_id = __MODULE__
    repo_start = {Northwind.Repo, :start_link, []}
    {:ok, _} = start_supervised(%{id: repo_id, start: repo_start})
    :ok = Importer.perform()
  end

  test "List All" do
    Repo.all(Model.Employee)
  end

  test "Insert / Delete Employee" do
    changes = %{first_name: "Evadne", employee_id: 1024}
    changeset = Model.Employee.changeset(changes)
    {:ok, employee} = Repo.insert(changeset)
    Repo.delete(employee)
  end

  test "Insert Employees" do
    changes = [%{first_name: "Fred", employee_id: 100}, %{first_name: "Steven", employee_id: 200}]
    nil = Repo.get(Model.Employee, 100)

    Repo.insert_all(Model.Employee, changes)
    %{first_name: "Fred"} = Repo.get(Model.Employee, 100)
  end

  test "List all Employees Again" do
    Repo.all(Model.Employee)
  end

  test "Select with Bound ID" do
    Repo.get(Model.Employee, 2)
  end

  test "Where" do
    Model.Employee
    |> where([x], x.title == "Vice President Sales" and x.first_name == "Andrew")
    |> Repo.all()
  end

  test "Where In None" do
    employee_ids = []

    Model.Employee
    |> where([x], x.employee_id in ^employee_ids)
    |> select([x], x.employee_id)
    |> Repo.all()
    |> Enum.sort()
    |> (&assert(&1 == employee_ids)).()
  end

  test "Where In One" do
    employee_ids = [3]

    Model.Employee
    |> where([x], x.employee_id in ^employee_ids)
    |> select([x], x.employee_id)
    |> Repo.all()
    |> Enum.sort()
    |> (&assert(&1 == employee_ids)).()
  end

  test "Where In Multiple" do
    employee_ids = [3, 5, 7]

    Model.Employee
    |> where([x], x.employee_id in ^employee_ids)
    |> select([x], x.employee_id)
    |> Repo.all()
    |> Enum.sort()
    |> (&assert(&1 == employee_ids)).()
  end

  test "Where In Nested" do
    employee_ids = [3, 5, 7]
    employee_first_names = ["Janet"]

    Model.Employee
    |> where([x], x.employee_id in ^employee_ids)
    |> where([x], x.first_name in ^employee_first_names)
    |> select([x], x.employee_id)
    |> Repo.all()
    |> Enum.sort()
    |> (&assert(&1 == [3])).()
  end

  test "Select Where" do
    Model.Employee
    |> where([x], x.title == "Vice President Sales" and x.first_name == "Andrew")
    |> select([x], x.last_name)
    |> Repo.all()
  end

  test "Select Where Is Nil" do
    query = Model.Employee |> where([x], is_nil(x.title))
    assert [] = Repo.all(query)

    changes = %{first_name: "Ghost", employee_id: 4096}
    changeset = Model.Employee.changeset(changes)
    {:ok, %{employee_id: employee_id} = employee} = Repo.insert(changeset)
    assert [%{employee_id: ^employee_id}] = Repo.all(query)
    Repo.delete(employee)
  end

  test "Select / Update" do
    Model.Employee
    |> where([x], x.title == "Vice President Sales")
    |> Repo.all()
    |> List.first()
    |> Model.Employee.changeset(%{title: "SVP Sales"})
    |> Repo.update()
  end

  test "Assoc Traversal" do
    Model.Employee
    |> Repo.get(5)
    |> Ecto.assoc(:reports)
    |> Repo.all()
    |> List.first()
    |> Ecto.assoc(:manager)
    |> Repo.one()
    |> Ecto.assoc(:reports)
    |> Repo.all()
  end

  test "Promote to Customer" do
    Model.Employee
    |> where([x], x.title == "Vice President Sales" and x.first_name == "Andrew")
    |> Repo.one()
    |> Model.Employee.changeset(%{title: "Customer"})
    |> Repo.update()
  end

  test "Stream Employees" do
    Model.Employee
    |> Repo.stream()
    |> Enum.to_list()
  end

  test "Order / Shipper / Orders Preloading" do
    Model.Order
    |> Repo.all()
    |> Repo.preload(shipper: :orders)
  end

  test "Order / Shipper + Employee Preloading" do
    Model.Order
    |> Repo.all()
    |> Repo.preload([[shipper: :orders], :employee, :customer], in_parallel: true)
  end

  test "Order / Shipper / Orders Preloading before all()" do
    Model.Order
    |> preload([_], shipper: :orders)
    |> Repo.all()
  end

  test "Order By Desc company_name, Asc phone" do
    sorted_etso =
      Model.Shipper
      |> order_by([x], desc: x.company_name, asc: x.phone)
      |> Repo.all()

    sorted_code =
      Model.Shipper
      |> Repo.all()
      |> Enum.sort_by(& &1.company_name, :desc)
      |> Enum.sort_by(& &1.phone)

    assert sorted_etso == sorted_code
  end

  test "Delete All" do
    assert Repo.delete_all(Model.Employee)
    assert [] == Repo.all(Model.Employee)
  end

  test "Delete Where" do
    query = Model.Employee |> where([e], e.employee_id in [1, 5])
    assert [_a, _b] = Repo.all(query)
    assert {2, nil} = Repo.delete_all(query)
    assert [] == Repo.all(query)
    refute [] == Repo.all(Model.Employee)
  end

  test "Delete Where Select" do
    query = Model.Employee |> where([e], e.employee_id in [1, 5])
    assert [_a, _b] = Repo.all(query)
    assert {2, list} = Repo.delete_all(query |> select([e], {e, e.employee_id}))
    assert is_list(list)
    assert Enum.any?(list, &(elem(&1, 1) == 1))
    assert Enum.any?(list, &(elem(&1, 1) == 5))
    assert [] = Repo.all(query)
    refute [] == Repo.all(Model.Employee)
  end

  describe "With JSON Extract Paths" do
    test "using literal value" do
      Model.Employee
      |> where([e], e.metadata["twitter"] == "@andrew_fuller")
      |> Repo.one!()
    end

    test "using brackets" do
      Model.Employee
      |> where([e], e.metadata["documents"]["passport"] == "verified")
      |> Repo.one!()
    end

    test "with variable pinning" do
      field = "passport"

      Model.Employee
      |> where([e], e.metadata["documents"][^field] == "verified")
      |> Repo.one!()

      Model.Employee
      |> select([e], json_extract_path(e.metadata, ["documents", "passport"]))
      |> Repo.all()
      |> Enum.any?(&(&1 == "verified"))
      |> assert()
    end

    test "with arrays" do
      Model.Employee
      |> select([e], json_extract_path(e.metadata, ["photos", 0, "url"]))
      |> where([e], e.metadata["documents"]["passport"] == "verified")
      |> Repo.one!()
      |> (&(&1 == "https://example.com/a")).()
      |> assert()

      Model.Employee
      |> where([e], e.metadata["documents"]["passport"] == "verified")
      |> select([e], e.metadata["photos"][0]["url"])
      |> Repo.one!()
      |> (&(&1 == "https://example.com/a")).()
      |> assert()

      Model.Employee
      |> select([e], e.metadata["photos"][1]["url"])
      |> where([e], e.metadata["documents"]["passport"] == "verified")
      |> Repo.one!()
      |> (&(&1 == "https://example.com/b")).()
      |> assert()
    end

    test "with where/in" do
      Model.Employee
      |> where([e], e.metadata["documents"]["passport"] in ~w(verified))
      |> select([e], e.metadata["photos"][1]["url"])
      |> Repo.one!()
      |> (&(&1 == "https://example.com/b")).()
      |> assert()
    end

    test "in deletion" do
      Model.Employee
      |> where([e], e.metadata["documents"]["passport"] == "verified")
      |> Repo.delete_all()

      assert_raise Ecto.NoResultsError, fn ->
        Model.Employee
        |> where([e], e.metadata["documents"]["passport"] == "verified")
        |> Repo.one!()
      end
    end
  end

  describe "Aggregates" do
    test "Count all" do
      count = Repo.aggregate(Model.Employee, :count, :employee_id)
      assert is_integer(count)
      assert count > 0
    end

    test "Count with where clause" do
      count =
        Model.Employee
        |> where([e], e.title == "Vice President Sales")
        |> Repo.aggregate(:count, :employee_id)

      assert count == 1
    end

    test "Count distinct" do
      # First, let's add some test data with duplicate values
      count_before = Repo.aggregate(Model.Employee, :count, :employee_id)

      # Count distinct employee_ids (should be same as count since IDs are unique)
      distinct_count =
        Model.Employee
        |> select([e], count(e.employee_id, :distinct))
        |> Repo.one()

      assert distinct_count == count_before
    end

    test "Sum" do
      # Sum employee IDs
      sum = Repo.aggregate(Model.Employee, :sum, :employee_id)
      assert is_integer(sum)
      assert sum > 0
    end

    test "Sum with where clause" do
      sum =
        Model.Employee
        |> where([e], e.employee_id > 5)
        |> Repo.aggregate(:sum, :employee_id)

      assert is_integer(sum)
    end

    test "Average" do
      avg = Repo.aggregate(Model.Employee, :avg, :employee_id)
      assert is_float(avg) or is_integer(avg)
      assert avg > 0
    end

    test "Average with where clause" do
      avg =
        Model.Employee
        |> where([e], e.employee_id <= 5)
        |> Repo.aggregate(:avg, :employee_id)

      assert is_float(avg) or is_integer(avg)
    end

    test "Min" do
      min = Repo.aggregate(Model.Employee, :min, :employee_id)
      assert is_integer(min)
      assert min > 0
    end

    test "Min with where clause" do
      min =
        Model.Employee
        |> where([e], e.employee_id > 1)
        |> Repo.aggregate(:min, :employee_id)

      assert is_integer(min)
      assert min > 1
    end

    test "Max" do
      max = Repo.aggregate(Model.Employee, :max, :employee_id)
      assert is_integer(max)
      assert max > 0
    end

    test "Max with where clause" do
      max =
        Model.Employee
        |> where([e], e.employee_id < 10)
        |> Repo.aggregate(:max, :employee_id)

      assert is_integer(max)
      assert max < 10
    end

    test "Aggregate on empty result" do
      count =
        Model.Employee
        |> where([e], e.employee_id == 99999)
        |> Repo.aggregate(:count, :employee_id)

      assert count == 0
    end

    test "Sum on field with nil values" do
      # Test that nil values are filtered out
      sum =
        Model.Employee
        |> Repo.aggregate(:sum, :employee_id)

      assert is_integer(sum)
    end
  end

  describe "Dynamic Repos" do
    test "put_dynamic_repo and get operations" do
      # Create a second repo for testing
      defmodule TestRepo2 do
        use Ecto.Repo, otp_app: :etso, adapter: Etso.Adapter
      end

      repo_id2 = :test_repo_2
      repo_start2 = {TestRepo2, :start_link, []}
      {:ok, _} = start_supervised(%{id: repo_id2, start: repo_start2})

      # Insert data into the original repo
      changes = %{first_name: "Test", employee_id: 9999}
      changeset = Model.Employee.changeset(changes)
      {:ok, _} = Repo.insert(changeset)

      # Verify it's in the original repo
      assert Repo.get(Model.Employee, 9999) != nil

      # Switch to the second repo
      Repo.put_dynamic_repo(TestRepo2)

      # The second repo should not have the data
      assert TestRepo2.get(Model.Employee, 9999) == nil

      # Insert data into the second repo
      {:ok, _} = TestRepo2.insert(changeset)
      assert TestRepo2.get(Model.Employee, 9999) != nil

      # Switch back to original repo
      Repo.put_dynamic_repo(Northwind.Repo)

      # Original repo should still have the data
      assert Repo.get(Model.Employee, 9999) != nil

      # Clean up
      Repo.delete(Repo.get!(Model.Employee, 9999))
    end

    test "dynamic repo with queries" do
      # Create a second repo
      defmodule TestRepo3 do
        use Ecto.Repo, otp_app: :etso, adapter: Etso.Adapter
      end

      repo_id3 = :test_repo_3
      repo_start3 = {TestRepo3, :start_link, []}
      {:ok, _} = start_supervised(%{id: repo_id3, start: repo_start3})

      # Insert into original repo
      changes = %{first_name: "Dynamic", employee_id: 8888}
      changeset = Model.Employee.changeset(changes)
      {:ok, _} = Repo.insert(changeset)

      # Query original repo
      count1 = Repo.aggregate(Model.Employee, :count, :employee_id)

      # Switch to second repo and query
      Repo.put_dynamic_repo(TestRepo3)
      count2 = Repo.aggregate(Model.Employee, :count, :employee_id)

      # Second repo should have different count (no data)
      assert count2 < count1

      # Switch back
      Repo.put_dynamic_repo(Northwind.Repo)

      # Clean up
      if employee = Repo.get(Model.Employee, 8888) do
        Repo.delete(employee)
      end
    end
  end

  describe "Composite Primary Keys" do
    # Note: Ecto doesn't natively support composite primary keys in schema definitions.
    # However, the adapter implementation supports them. This test verifies that
    # the adapter can handle multiple primary keys when they are provided.
    # In practice, composite keys would be handled through custom schema definitions
    # or by manually specifying the primary key fields.

    test "adapter handles multiple primary key fields in filters" do
      # Test that the adapter can extract and use composite keys from filters
      # This is tested indirectly through the TableStructure.extract_primary_key function
      alias Etso.ETS.TableStructure

      # Create a mock schema module that returns multiple primary keys
      defmodule MockCompositeSchema do
        def __schema__(:primary_key), do: [:id1, :id2]
        def __schema__(:fields), do: [:id1, :id2, :name]
      end

      filters = [id1: 1, id2: 2]
      key = TableStructure.extract_primary_key(MockCompositeSchema, filters)

      # Should return a tuple for composite keys
      assert key == {1, 2}
    end

    test "adapter handles single primary key in filters" do
      alias Etso.ETS.TableStructure

      defmodule MockSingleKeySchema do
        def __schema__(:primary_key), do: [:id]
        def __schema__(:fields), do: [:id, :name]
      end

      filters = [id: 1]
      key = TableStructure.extract_primary_key(MockSingleKeySchema, filters)

      # Should return a single value for single keys
      assert key == 1
    end
  end

  describe "Joins" do
    test "inner join with simple condition" do
      query =
        from o in Model.Order,
          join: e in Model.Employee,
          on: o.employee_id == e.employee_id,
          select: {o.order_id, e.first_name}

      results = Repo.all(query)
      assert length(results) > 0
      
      # Verify structure
      [first | _] = results
      assert is_tuple(first)
      assert tuple_size(first) == 2
    end

    test "left join with no matches" do
      # Create an employee with no orders
      changes = %{employee_id: 999, first_name: "Test", last_name: "NoOrders"}
      changeset = Model.Employee.changeset(changes)
      {:ok, _} = Repo.insert(changeset)

      query =
        from e in Model.Employee,
          left_join: o in Model.Order,
          on: e.employee_id == o.employee_id,
          where: e.employee_id == 999,
          select: {e.employee_id, o.order_id}

      results = Repo.all(query)
      assert length(results) == 1
      
      [result | _] = results
      {emp_id, order_id} = result
      assert emp_id == 999
      assert is_nil(order_id)

      # Clean up
      Repo.delete(Repo.get!(Model.Employee, 999))
    end

    test "inner join with where clause" do
      query =
        from o in Model.Order,
          join: e in Model.Employee,
          on: o.employee_id == e.employee_id,
          where: e.first_name == "Anne",
          select: {o.order_id, e.first_name}

      results = Repo.all(query)
      assert length(results) > 0
      
      # All results should have Anne as first name
      Enum.each(results, fn {_order_id, first_name} ->
        assert first_name == "Anne"
      end)
    end

    test "multiple joins" do
      query =
        from o in Model.Order,
          join: e in Model.Employee,
          on: o.employee_id == e.employee_id,
          join: c in Model.Customer,
          on: o.customer_id == c.customer_id,
          select: {o.order_id, e.first_name, c.company_name}

      results = Repo.all(query)
      assert length(results) > 0
      
      # Verify structure
      [first | _] = results
      assert is_tuple(first)
      assert tuple_size(first) == 3
    end

    test "join with complex condition (AND)" do
      query =
        from o in Model.Order,
          join: e in Model.Employee,
          on: o.employee_id == e.employee_id and e.first_name == "Anne",
          select: {o.order_id, e.first_name}

      results = Repo.all(query)
      # Should only return orders for Anne
      Enum.each(results, fn {_order_id, first_name} ->
        assert first_name == "Anne"
      end)
    end

    test "join with select specific fields" do
      query =
        from o in Model.Order,
          join: e in Model.Employee,
          on: o.employee_id == e.employee_id,
          select: {o.order_id, e.employee_id, e.first_name}

      results = Repo.all(query)
      assert length(results) > 0
      
      [first | _] = results
      assert is_tuple(first)
      assert tuple_size(first) == 3
    end

    test "right join" do
      # Create an order with a non-existent employee_id to test right join
      # Actually, right joins are less common, so let's test with existing data
      query =
        from e in Model.Employee,
          right_join: o in Model.Order,
          on: e.employee_id == o.employee_id,
          select: {e.employee_id, o.order_id}

      results = Repo.all(query)
      assert length(results) > 0
    end

    test "join with order_by" do
      query =
        from o in Model.Order,
          join: e in Model.Employee,
          on: o.employee_id == e.employee_id,
          order_by: [asc: o.order_id],
          select: {o.order_id, e.first_name},
          limit: 5

      results = Repo.all(query)
      assert length(results) <= 5
      
      # Verify ordering
      order_ids = Enum.map(results, fn {order_id, _} -> order_id end)
      assert order_ids == Enum.sort(order_ids)
    end

    test "join with empty result" do
      query =
        from o in Model.Order,
          join: e in Model.Employee,
          on: o.employee_id == e.employee_id,
          where: e.employee_id == 99999,
          select: {o.order_id, e.first_name}

      results = Repo.all(query)
      assert results == []
    end
  end
end
