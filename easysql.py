# -*- coding: utf-8 -*-

import os
from functools import cached_property

import tablib
from sqlalchemy import MetaData
from sqlalchemy import Table as SaTable
from sqlalchemy import create_engine, text
from sqlalchemy.sql import select


class Row:
    """A row from a table of a database."""

    def __init__(self, keys: list, values: list, table=None, database=None):
        self._keys = keys
        self._values = values
        self._table = table
        self._database = database
        assert len(self._keys) == len(self._values)

    def __repr__(self) -> str:
        return '<Row {}>'.format(str(self))

    def __str__(self) -> str:
        return self.export('json')[1:-1]

    def __len__(self) -> int:
        return len(self._keys)

    def __getitem__(self, key):
        # Support for index-based lookup.
        if isinstance(key, int):
            return self.values()[key]

        # Support for key-based lookup.
        if key in self.keys():
            i = self.keys().index(key)
            if self.keys().count(key) > 1:
                raise KeyError("Multiple '{}' fields.".format(key))
            return self.values()[i]

        raise KeyError("No '{}' field.".format(key))

    # TODO: fix the conflict between __getattr__ and @property
    # def __getattr__(self, key):
    #     try:
    #         return self[key]
    #     except KeyError as e:
    #         raise AttributeError(e)

    def __iter__(self):
        for z in zip(self._keys, self._values):
            yield z

    @property
    def dataset(self) -> tablib.Dataset:
        """A Tablib Dataset representation of the Row."""
        data = tablib.Dataset(headers=self.keys())
        data.append(self.values(reduce_datetimes=True))
        return data

    @property
    def table(self):
        """Returns the related table of the row. If not a single table related, returns None."""
        return self._table

    @property
    def database(self):
        """Returns the related database of the row."""
        return self._database

    def keys(self):
        """Returns the list of column names of the table."""
        return self._keys

    def values(self, reduce_datetimes=False):
        """Returns the list of values from the query."""
        return self._reduce_datetimes(self._values) if reduce_datetimes else self._values

    def get(self, key, default=None):
        """Returns the value for a given key, or default."""
        try:
            return self[key]
        except KeyError:
            return default

    def set(self, key, value):
        """Sets the value of the key, without saving to database."""
        i = self._keys.index(key)
        self._values[i] = value

    def save(self) -> bool:
        """Saves changes to database based on the primary key."""
        pass

    def delete(self) -> bool:
        """Deletes the row from database based on the primary key."""
        pass

    def export(self, format, **kwargs):
        """Exports the row to the given format."""
        return self.dataset.export(format, **kwargs)

    @staticmethod
    def _reduce_datetimes(input: list) -> list:
        """Receives a list and converts datetimes to strings in it."""
        return [r.isoformat() if hasattr(r, 'isoformat') else r for r in input]


class RowSet:
    """A set of rows from a table of a database."""

    def __init__(self, rows):
        self._pre_rows = rows
        self._all_rows = []
        self.pending = True

    def __repr__(self):
        return '<RowSet fetched={} pending={}>'.format(len(self), self.pending)

    def __len__(self):
        return len(self._all_rows)

    def __iter__(self):
        """Iterate over all rows, consuming the underlying generator only when necessary."""
        i = 0
        while True:
            if i < len(self):
                # Check the cached _all_rows first.
                yield self[i]
            else:
                # Enter the generator _pre_rows and throw StopIteration when done.
                try:
                    yield next(self)
                except StopIteration:
                    return
            i += 1

    def __next__(self):
        try:
            nextrow = next(self._pre_rows)
            self._all_rows.append(nextrow)
            return nextrow
        except StopIteration:
            self.pending = False
            raise StopIteration('RowSet contains no more rows.')

    def __getitem__(self, key):
        is_int = isinstance(key, int)

        # Convert int into slice.
        sli = slice(key, key + 1) if is_int else key

        while sli.stop is None or len(self) < sli.stop:
            # Turn enough generator _pre_rows into cached _all_rows.
            try:
                next(self)
            except StopIteration:
                break

        return self._all_rows[key] if is_int else RowSet(iter(self._all_rows[key]))

    @property
    def dataset(self) -> tablib.Dataset:
        """A Tablib Dataset representation of the RowSet."""
        # Create a new Tablib Dataset.
        data = tablib.Dataset()

        try:
            # Set the column names as headers on Tablib Dataset.
            data.headers = self[0].keys()
        except IndexError:
            # If the RowSet is empty, just return the empty set.
            return data

        # Set rows.
        for row in self:
            data.append(row.values(reduce_datetimes=True))

        return data

    def all(self):
        list(self)
        return self

    def first(self) -> Row:
        """Returns the first Row of the RowSet. If there is nothing in the RowSet,
        then raise an IndexError."""
        return self[0]

    def one(self) -> Row:
        """Returns the first Row of the RowSet, ensuring that it is the only row."""
        if len(list(self)) > 1:
            raise ValueError('Contains more than one row.')

        return self.first()

    def scalar(self):
        """Returns the first column of the first row, or `default`."""
        return self.first()[0]

    def export(self, format, **kwargs):
        """Exports all the rows in the RowSet to the given format."""
        return self.dataset.export(format, **kwargs)


class Executor:
    def __init__(self, table, clause):
        self._table = table
        self._name = str(clause).split()[0]
        self._pre = clause

    def where(self, clause):
        self._pre = self._pre.where(clause)
        return self

    def execute(self):
        return self._table.database.execute(self._pre)


class Table:
    def __init__(self, table: SaTable, database):
        self._table = table
        self._database = database

    def __repr__(self):
        return '<Table {}>'.format(self.name)

    def __len__(self):
        return len(self._table.columns)

    def __getitem__(self, key):
        if key in self._table.columns.keys():
            return self._table.columns[key]

        raise KeyError("No '{}' column.".format(key))

    @property
    def name(self) -> str:
        return self._table.name

    @property
    def database(self):
        return self._database

    @property
    def columns(self):
        return self._table.columns

    @property
    def c(self):
        return self.columns

    def query(self, query, **params):
        return self.database.query(query, **params)

    def insert(self, *args, **kwargs):
        """Insert rows into the table.
        Examples:
            insert(id=1, name='Tom', email='tom@example.com')
            insert({'id': 1, 'name': 'Tom', 'email'='tom@example.com'})
            insert([
                {'id': 1, 'name': 'Tom', 'email'='tom@example.com'},
                {'id': 2, 'name': 'Kat', 'email'='kat@example.com'},
            ])
        """
        command = self._table.insert()
        if len(kwargs) > 0:
            return self._database.execute(command.values(**kwargs))
        else:
            return self._database.execute(command, args[0])

    def delete(self, **kwargs) -> Executor:
        return Executor(table=self, clause=self._table.delete().values(**kwargs))

    def update(self, **kwargs):
        return Executor(table=self, clause=self._table.update().values(**kwargs))

    def select(self, *args):
        if len(args) == 0:
            return Executor(table=self, clause=select([self._table]))

        array = [a.gg if isinstance(a, str) else a for a in args]
        return Executor(table=self, clause=select(array))

    def join(self):
        pass


class Database:
    def __init__(self, db_url=None, **kwargs):
        self.db_url = db_url or os.environ.get('DATABASE_URL')
        if not self.db_url:
            raise ValueError('A db_url must be provided.')

        self._engine = create_engine(self.db_url, pool_pre_ping=True, **kwargs)
        self._conn = self._engine.connect()
        self._meta = MetaData()
        self._meta.reflect(bind=self._engine)
        self.open = True

    def __repr__(self):
        return '<Database open={}>'.format(self.open)

    @cached_property
    def table_names(self) -> list:
        """Returns a list of table names for the connected database."""
        return self._engine.table_names()

    def close(self):
        """Closes the Database."""
        self.open = False
        self._conn.close()
        self._engine.dispose()

    def execute(self, statement):
        """Executes the statement using the database connection."""
        return self._conn.execute(statement)

    def query(self, query, **params):
        """Executes the given SQL query against the connected Database.
        Parameters can, optionally, be provided. Returns a RowSet."""
        cursor = self._conn.execute(text(query), **params)

        return RowSet(Row(cursor.keys(), r, None, self) for r in cursor)

    def bulk_query(self):
        pass

    def get_table(self, name) -> Table:
        """Returns a connection to a table of the database."""
        if name not in self.table_names:
            raise KeyError("Unknown table name '{}'".format(name))

        return Table(self._meta.tables[name], self)
