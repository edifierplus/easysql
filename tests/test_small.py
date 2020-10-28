from easysql import Row
from pytest import raises
from datetime import datetime
from collections import OrderedDict
import json
import tablib


class TestRow:
    def create_standard_row(self):
        keys = ['id', 'name', 'email', 'birthday']
        values = [250, 'Fool', 'fake@unreal.abcd', datetime.fromtimestamp(0)]
        return Row(keys, values, "DummyTable", "DummyDatabase")

    def test___init___equal_length(self):
        keys = ['a', 'b', 'c']
        values = ['A', 'B', 'C']

        row = Row(keys, values)

        assert row._keys is keys
        assert row._values is values

    def test___init___unequal_length(self):
        keys = ['a', 'b', 'c']
        values = ['A', 'B']

        with raises(AssertionError):
            Row(keys, values)

    def test___init___with_table(self):
        pass

    def test___repr__(self):
        row = self.create_standard_row()
        expectation = '<Row {}>'.format(str(row))

        outcome = row.__repr__()

        assert outcome == expectation

    def test___str__(self):
        keys = ['a', 'b', 'c', 'd']
        values = [250, 'B', 'C', datetime.fromtimestamp(0).isoformat()]
        expectation = json.dumps(OrderedDict(zip(keys, values)))
        row = Row(keys, values)

        outcome = str(row)

        assert outcome == expectation

    def test___len__(self):
        row = self.create_standard_row()

        assert len(row) == len(row._keys)

    def test___getitem___index_based(self):
        row = self.create_standard_row()

        for i in range(len(row)):
            assert row[i] == row.values()[i]

    def test___getitem___key_based(self):
        row = self.create_standard_row()

        for key, value in zip(row._keys, row._values):
            assert row[key] == value

    def test___getitem___multiple_fields(self):
        row = Row(['a', 'b', 'b'], [1, 2, 3])

        with raises(KeyError, match="Multiple 'b' fields."):
            row['b']

    def test___getitem___no_field(self):
        row = self.create_standard_row()

        with raises(KeyError, match="No 'c' field."):
            row['c']

    def test___getattr__(self):
        pass

    def test___iter__(self):
        row = self.create_standard_row()

        for k, v in row:
            assert row[k] == v

    def test_dataset(self):
        row = self.create_standard_row()

        assert isinstance(row.dataset, tablib.Dataset)

    def test_table(self):
        row = self.create_standard_row()

        assert row.table is row._table

    def test_database(self):
        row = self.create_standard_row()

        assert row.database is row._database

    def test_keys(self):
        row = self.create_standard_row()

        assert row.keys() is row._keys

    def test_values(self):
        row = self.create_standard_row()

        assert row.values() is row._values

    def test_values_reduce_datetimes(self):
        row = self.create_standard_row()

        unconverted = row.values()
        converted = row.values(reduce_datetimes=True)

        assert unconverted[-1].isoformat() == converted[-1]

    def test_get(self):
        row = self.create_standard_row()

        for i in range(len(row)):
            assert row.get(i) == row.values()[i]

        for key, value in zip(row._keys, row._values):
            assert row.get(key) == value

    def test_get_fail_with_default(self):
        row = self.create_standard_row()

        assert row.get("absence", "default") == "default"

    def test_get_fail_without_default(self):
        row = self.create_standard_row()

        assert row.get("absence") is None

    def test_set(self):
        row = self.create_standard_row()
        for k in row.keys():
            row.set(k, k + "newvalue")
            assert row[k] == k + "newvalue"

        with raises(ValueError):
            row.set("absence", "newvalue")

    def test_save(self):
        pass

    def test_delete(self):
        pass

    def test_export(self):
        keys = ['a', 'b', 'c', 'd']
        values = [250, 'B', 'C', datetime.fromtimestamp(0).isoformat()]
        expectation = json.dumps([OrderedDict(zip(keys, values))])
        row = Row(keys, values)

        outcome = row.export('json')

        assert outcome == expectation

    def test__reduce_datetimes(self):
        t = datetime.now()
        origin = [t, 1, 'abc', True, None]
        expectation = origin.copy()
        expectation[0] = t.isoformat()

        outcome = Row._reduce_datetimes(origin)

        assert outcome == expectation


class TestRowSet:
    def test___init__(self):
        pass

    def test___repr__(self):
        pass

    def test___len__(self):
        pass

    def test___iter__(self):
        pass

    def test___next__(self):
        pass

    def test___getitem__(self):
        pass

    def test_dataset(self):
        pass

    def test_all(self):
        pass

    def test_first(self):
        pass

    def test_one(self):
        pass

    def test_scalar(self):
        pass

    def test_export(self):
        pass


class TestTable:
    def test___init__(self):
        pass

    def test___repr__(self):
        pass

    def test_name(self):
        pass

    def test_database(self):
        pass

    def test_query(self):
        pass

    def test_clear(self):
        pass

    def test_insert(self):
        pass

    def test_delete(self):
        pass

    def test_update(self):
        pass

    def test_select(self):
        pass

    def test_join(self):
        pass

    def test_where(self):
        pass

    def test_order_by(self):
        pass

    def test_execute(self):
        pass

    def test_(self):
        pass


class TestDatabase:
    def test___init__(self):
        pass

    def test_table_names(self):
        pass

    def test_close(self):
        pass

    def test_query(self):
        pass

    def test_bulk_query(self):
        pass

    def test_get_table(self):
        pass
