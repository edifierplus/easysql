# -*- coding: utf-8 -*-

import os
from collections import OrderedDict
from functools import cached_property
from records.setup import read
import tablib
from sqlalchemy import create_engine, exc, inspect, text


class Row:
    """A row from a table of a database."""

    def __init__(self, keys, schemas, values, table):
        self._keys = keys
        self._schemas = schemas
        self._values = values
        self._table = table
        assert len(self._keys) == len(self._schemas) == len(self._values)

    def __repr__(self) -> str:
        return '<Row {}>'.format(self.__str__())

    def __str__(self) -> str:
        return self.export('json')[1:-1]

    def __len__(self) -> int:
        return len(self._keys)

    def __getitem__(self, key):
        # Support for index-based lookup.
        if isinstance(key, int):
            return self.values()[key]

        # Support for string-based lookup.
        if key in self.keys():
            i = self.keys().index(key)
            if self.keys().count(key) > 1:
                raise KeyError("Multiple '{}' fields.".format(key))
            return self.values()[i]

        raise KeyError("No '{}' field.".format(key))

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(e)

    def __iter__(self):
        for z in zip(self._keys, self._values):
            yield z

    @property
    def dataset(self) -> tablib.Dataset:
        """A Tablib Dataset representation of the Row."""
        data = tablib.Dataset(headers=self.keys())
        data.append(_reduce_datetimes(self.values()))
        return data

    def keys(self):
        """Returns the list of column names of the table."""
        return self._keys

    def schemas(self):
        """Returns the list of schemas describing the keys."""
        return self._schemas

    def values(self):
        """Returns the list of values from the query."""
        return self._values

    def get(self, key, default=None):
        """Returns the value for a given key, or default."""
        try:
            return self[key]
        except KeyError:
            return default

    def set(self, key, value):
        """Sets the value of the key, without saving to database."""
        pass

    def save(self) -> bool:
        """Saves changes to database based on the primary key."""
        pass

    def delete(self) -> bool:
        """Deletes the row from database based on the primary key."""
        pass

    def export(self, format, **kwargs):
        """Exports the row to the given format."""
        return self.dataset.export(format, **kwargs)


class RowSet:
    """A set of rows from a table of a database."""

    def __init__(self, rows: list):
        self._rows = rows
        pass

    def __repr__(self):
        return '<RowSet size={}>'.format(len(self))

    def __len__(self):
        return len(self._all_rows)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._all_rows[key]
        else:
            return RowSet(self._rows[key])

    def __iter__(self):
        for r in self._rows:
            yield r

    @property
    def size(self) -> int:
        return len(self._all_rows)

    @property
    def dataset(self) -> tablib.Dataset:
        """A Tablib Dataset representation of the RowSet."""
        # Create a new Tablib Dataset.
        data = tablib.Dataset()

        # If the RowSet is empty, just return the empty set
        if len(self._rows) == 0:
            return data

        # Set the column names as headers on Tablib Dataset.
        data.headers = self[0].keys()

        # Set rows.
        for row in self.all():
            data.append(_reduce_datetimes(row.values()))

        return data

    def first(self, default=Exception):
        """Returns the first Row of the RowSet, or `default`. If
        `default` is a subclass of Exception, then raise IndexError
        instead of returning it."""
        try:
            return self[0]
        except IndexError as e:
            if issubclass(default, Exception):
                raise e
            return default

    def one(self):
        """Returns the first Row of the RowSet, ensuring that it is the only row."""
        if len(self) > 1:
            raise ValueError('Contains more than one row.')

        return self.first()

    def scalar(self):
        """Returns the first column of the first row, or `default`."""
        return self.first()[0]


class Table:
    def __init__(self, name, database):
        self._name = name
        self._database = database

    @property
    def name(self) -> str:
        return self._name

    @property
    def database(self):
        return self._database

    def insert(self):
        pass

    def delete(self):
        pass

    def update(self):
        pass

    def select(self):
        pass

    def join(self):
        pass

    def where(self):
        pass

    def order_by(self):
        pass

    def execute(self):
        pass


class Database:
    def __init__(self, db_url=None, **kwargs):
        self.db_url = db_url or os.environ.get('DATABASE_URL')
        if not self.db_url:
            raise ValueError('A db_url must be provided.')

        self._engine = create_engine(self.db_url, pool_pre_ping=True, **kwargs)
        self._conn = self._engine.connect()
        self.open = True

    def __repr__(self):
        return '<Database open={}>'.format(self.open)

    @cached_property
    def table_names(self) -> list:
        """Returns a list of table names for the connected database."""
        return inspect(self._engine).get_table_names()

    def close(self):
        """Closes the Database."""
        self.open = False
        self._conn.close()
        self._engine.dispose()

    def query(self):
        pass

    def bulk_query(self):
        pass

    def get_table(self, name) -> Table:
        """Returns a connection to a table of the database."""
        if name not in self.table_names:
            raise KeyError("Unknown table name '{}'".format(name))

        return Table(name, self)


def _reduce_datetimes(row: Row):
    """Receives a row, converts datetimes to strings."""
    return tuple(r.isoformat() if hasattr(r, 'isoformat') else r for r in row.values())
