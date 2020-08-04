import csv
import os
import typing

import sqlalchemy

from .utils import snakecase


def infer_type(value: typing.Any) -> str:
    """
    Infers Sqlite column types based on Python types
    """
    try:
        int(value)
        return "INTEGER"
    except ValueError:
        pass
    try:
        float(value)
        return "REAL"
    except ValueError:
        pass
    return "TEXT"


class CSVFile:
    """
    CSVFile
    """

    def __init__(self, path: str, encoding: str = 'utf-8', delimiter: str = ',', table_name: str = None,
                 drop_tables: bool = True,
                 headers: bool = True,
                 typing_style: str = None, bracket_style: str = "all", temp: bool = False, snakecase: bool = True):
        self.path = path
        self.encoding = encoding
        self.delimiter = delimiter
        self._table_name = table_name
        self.drop_tables = drop_tables
        self.headers = headers

        self.typing_style = typing_style

        self.file = None
        self.reader = None

        self.column_names = []
        self.column_types = []
        self.lb, self.rb = ("[", "]") if bracket_style == "all" else ("", "")
        self.auto_id = False
        self.temp = temp
        self.snakecase = snakecase

    def infer_types(self):
        self.restart()
        if self.headers:
            self.column_names = [snakecase(name) if self.snakecase else name for name in next(self.reader)]
        else:
            self.column_names = [f"column_{idx}" for idx in range(1, len(next(self.reader)) + 1)]

        col_count = len(self.column_names)

        if not self.typing_style:
            self.column_types = ["TEXT"] * col_count
        else:
            self.column_types = ["INTEGER"] * col_count
            for row in self.reader:
                for col in range(col_count):
                    if self.column_types[col] == "TEXT":
                        continue
                    col_type = infer_type(row[col])
                    if self.column_types[col] != col_type:
                        if col_type == "TEXT" or \
                                (col_type == "REAL" and self.column_types[col] == "INTEGER"):
                            self.column_types[col] = col_type
                if self.typing_style == 'quick':
                    break

        if 'id' not in self.column_names:
            self.auto_id = True
            self.column_names.insert(0, 'id')
            self.column_types.insert(0, 'INTEGER PRIMARY KEY')

    def save_to_db(self, connection: sqlalchemy.engine.Connection):
        col_count = len(self.column_names)

        if self.drop_tables:
            try:
                connection.execute('DROP TABLE [{table_name}]'.format(table_name=self.table_name))
            except:
                pass
        create_table_query = 'CREATE TABLE [{table_name}] (\n'.format(table_name=self.table_name) \
                             + ',\n'.join(
            "\t%s%s%s %s" % (self.lb, i[0], self.rb, i[1]) for i in zip(self.column_names, self.column_types)) \
                             + '\n);'
        connection.execute(create_table_query)

        linesTotal = 0
        currentBatch = 0
        self.restart()
        buf = []
        maxL = 10000
        if self.headers:
            next(self.reader)  # skip headers
        for line in self.reader:
            if self.auto_id:
                buf.append([None, *line])
            else:
                buf.append(line)

            currentBatch += 1
            if currentBatch == maxL:
                # write_out("Inserting {0} records into {1}".format(maxL, self.get_table_name()))
                connection.executemany('INSERT INTO [{table_name}] VALUES ({cols})'
                                       .format(table_name=self.table_name, cols=','.join(['?'] * col_count)),
                                       buf)
                linesTotal += currentBatch
                currentBatch = 0
                buf = []
        if len(buf) > 0:
            # write_out("Flushing the remaining {0} records into {1}".format(len(buf), self.get_table_name()))
            connection.executemany('INSERT INTO [{table_name}] VALUES ({cols})'
                                   .format(table_name=self.table_name, cols=','.join(['?'] * col_count)),
                                   buf)
            linesTotal += len(buf)
        return linesTotal

    @property
    def file_name(self):
        return os.path.splitext(os.path.basename(self.path))[0]

    @property
    def table_name(self):
        if self._table_name:
            return self._table_name
        else:
            if self.snakecase:
                return snakecase(os.path.splitext(os.path.basename(self.path))[0])
            else:
                return os.path.splitext(os.path.basename(self.path))[0]

    def open(self):
        self.file = open(self.path, encoding=self.encoding)
        self.reader = csv.reader(self.file, delimiter=self.delimiter)

    def restart(self):
        self.file.seek(0)

    def close(self):
        if self.file:
            self.file.close()
            if self.temp:
                self.remove()

    def remove(self):
        os.remove(self.path)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *args):
        self.close()


def extract_file(db_path: str, filepath: str, **kwargs):
    """
    Extracts data from file and inserts into database
    """
    engine = sqlalchemy.create_engine(f'sqlite:///{db_path}')
    # Get raw DBAPI2 connection from SQLAlchemy
    connection = engine.pool._creator()
    with CSVFile(filepath, **kwargs) as file:
        file.infer_types()
        file.save_to_db(connection)
    connection.commit()


def drop_table(db_path: str, table_name: str):
    engine = sqlalchemy.create_engine(f'sqlite:///{db_path}')
    # Get raw DBAPI2 connection from SQLAlchemy
    connection = engine.pool._creator()
    connection.execute('DROP TABLE [{table_name}]'.format(table_name=table_name))