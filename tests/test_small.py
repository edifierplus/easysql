import json
from collections import OrderedDict
from datetime import datetime

import tablib
from easysql import Database, Row, RowSet, Table
from pytest import fixture, raises


@fixture
def standard_row():
    keys = ['id', 'name', 'email', 'birthday']
    values = [250, 'Fool', 'fake@unreal.abcd', datetime.fromtimestamp(0)]
    return Row(keys, values, "DummyTable", "DummyDatabase")


@fixture
def standard_database():
    return Database(db_url="sqlite:///tests/db.sqlite3")


@fixture
def standard_rowset():
    db = Database(db_url="sqlite:///tests/db.sqlite3")
    return db.query(("SELECT * FROM display_signal"))


class TestRow:
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

    def test___init___with_table_and_database(self, standard_row):
        assert standard_row._table == "DummyTable"
        assert standard_row._database == "DummyDatabase"

    def test___repr__(self, standard_row):
        expectation = '<Row {}>'.format(str(standard_row))

        outcome = standard_row.__repr__()

        assert outcome == expectation

    def test___str__(self):
        keys = ['a', 'b', 'c', 'd']
        values = [250, 'B', 'C', datetime.fromtimestamp(0).isoformat()]
        expectation = json.dumps(OrderedDict(zip(keys, values)))
        row = Row(keys, values)

        outcome = str(row)

        assert outcome == expectation

    def test___len__(self, standard_row):
        assert len(standard_row) == len(standard_row._keys)

    def test___getitem___index_based(self, standard_row):
        for i in range(len(standard_row)):
            assert standard_row[i] == standard_row.values()[i]

    def test___getitem___key_based(self, standard_row):
        for key, value in zip(standard_row._keys, standard_row._values):
            assert standard_row[key] == value

    def test___getitem___multiple_fields(self):
        row = Row(['a', 'b', 'b'], [1, 2, 3])

        with raises(KeyError, match="Multiple 'b' fields."):
            row['b']

    def test___getitem___no_field(self, standard_row):
        with raises(KeyError, match="No 'c' field."):
            standard_row['c']

    def test___getattr__(self):
        pass

    def test___iter__(self, standard_row):
        for k, v in standard_row:
            assert standard_row[k] == v

    def test_dataset(self, standard_row):
        assert isinstance(standard_row.dataset, tablib.Dataset)

    def test_table(self, standard_row):
        assert standard_row.table is standard_row._table

    def test_database(self, standard_row):
        assert standard_row.database is standard_row._database

    def test_keys(self, standard_row):
        assert standard_row.keys() is standard_row._keys

    def test_values(self, standard_row):
        assert standard_row.values() is standard_row._values

    def test_values_reduce_datetimes(self, standard_row):
        unconverted = standard_row.values()
        converted = standard_row.values(reduce_datetimes=True)

        assert unconverted[-1].isoformat() == converted[-1]

    def test_get(self, standard_row):
        for i in range(len(standard_row)):
            assert standard_row.get(i) == standard_row.values()[i]

        for key, value in zip(standard_row._keys, standard_row._values):
            assert standard_row.get(key) == value

    def test_get_failed(self, standard_row):
        assert standard_row.get("absence", "default") == "default"
        assert standard_row.get("absence") is None

    def test_set(self, standard_row):
        for k in standard_row.keys():
            standard_row.set(k, k + "newvalue")
            assert standard_row[k] == k + "newvalue"

        with raises(ValueError):
            standard_row.set("absence", "newvalue")

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
        rows = (i for i in range(10))
        rowset = RowSet(rows)

        assert rowset._pre_rows == rows
        assert rowset._all_rows == []
        assert rowset.pending is True

    def test___repr__(self, standard_rowset):
        assert standard_rowset.__repr__() == '<RowSet fetched=0 pending=True>'

    def test___len__(self, standard_rowset):
        assert len(standard_rowset) == 0

        standard_rowset.first()

        assert len(standard_rowset) == 1

    def test___iter__(self, standard_rowset):
        row_list = list(standard_rowset)

        assert len(row_list) == len(standard_rowset)
        assert standard_rowset.pending is False

    def test___next__(self, standard_rowset):
        with raises(StopIteration):
            row_list = []
            while True:
                row_list.append(next(standard_rowset))
                assert len(row_list) == len(standard_rowset)

    def test___getitem__(self, standard_rowset):
        rowset = RowSet((i for i in range(10)))

        index_result = rowset[5]
        slice_result = rowset[2:7]

        assert isinstance(index_result, int)
        assert index_result == 5
        assert isinstance(slice_result, RowSet)
        assert list(slice_result) == list(range(2, 7))

    def test_dataset(self, standard_rowset):
        assert isinstance(standard_rowset.dataset, tablib.Dataset)

    def test_all(self, standard_rowset):
        standard_rowset.all()

        assert standard_rowset.pending is False

    def test_first(self, standard_rowset):
        assert standard_rowset.first() is standard_rowset[0]

    def test_first_failed(self):
        empty_rowset = RowSet(iter(()))

        with raises(IndexError):
            empty_rowset.first()

    def test_one(self):
        rowset = RowSet((i for i in range(8, 9)))

        assert rowset.one() == 8

    def test_one_failed(self):
        rowset = RowSet((i for i in range(10)))

        with raises(ValueError, match='Contains more than one row.'):
            rowset.one()

    def test_scalar(self):
        rowset = RowSet(([str(i)] for i in range(10)))

        assert rowset.scalar() == '0'

    def test_export(self):
        pass


class TestTable:
    def test___init__(self):
        pass

    def test___repr__(self):
        pass

    def test___len__(self):
        pass

    def test___getitem__(self):
        pass

    def test_name(self):
        pass

    def test_database(self):
        pass

    def test_columns(self):
        pass

    def test_c(self):
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


class TestDatabase:
    def test___init__(self):
        database = Database("sqlite://")

        assert database.open is True

    def test___repr__(self, standard_database):
        assert standard_database.__repr__() == '<Database open=True>'

    def test_table_names(self, standard_database):
        assert standard_database.table_names[:3] == ['auth_group', 'auth_group_permissions', 'auth_permission']

    def test_close(self):
        database = Database("sqlite://")
        database.close()

        assert database.open is False
        assert database._conn.closed is True

    def test_query(self, standard_database):
        ans = standard_database.query("SELECT * FROM display_signal")

        assert isinstance(ans, RowSet)

    def test_bulk_query(self):
        pass

    def test_get_table(self, standard_database):
        table = standard_database.get_table('display_signal')

        assert isinstance(table, Table)
        assert table.name == 'display_signal'

    def test_get_table_failed(self, standard_database):
        with raises(KeyError, match="Unknown table name 'unknown'"):
            standard_database.get_table('unknown')
