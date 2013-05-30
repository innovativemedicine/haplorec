import unittest
import oursql
import re
import tempfile
import shutil
import os.path
import csv
from StringIO import StringIO

class DatabaseTestMixin(object):
    # NOTE: we can't use an __init__ method with a non-empty constructor, 
    # since it doesn't play nice with the unittest test-runner
    host = 'localhost'
    port = 3306
    user = 'root'
    passwd = ''
    db = None
    schema_file = None
    schema = None
    connection = None

    def connect(self, recreate=True):
        if self.schema_file is not None or self.schema is not None and recreate:
            init_db = oursql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                passwd=self.passwd,
            )
            c = init_db.cursor()
            c.execute("drop database if exists {db}".format(db=self.db))
            c.execute("create database {db}".format(db=self.db))
            c.execute("use {db}".format(db=self.db))
            for line in _lines(self.schema, self.schema_file):
                c.execute(line)
            init_db.close()

        self.connection = oursql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            db=self.db,
        )
        self.cursor = self.connection.cursor()

    def drop(self):
        self.cursor.execute("drop database if exists {db}".format(db=self.db))
        self.connection.close()

def _lines(schema, schema_file):
    schema_str = None
    if type(schema_file) == str:
        # filename
        schema_str = ''.join(open(schema_file).readlines())
    elif type(schema_file) == list:
        # lines in a schema
        schema_str = ''.join(schema_file)
    else:
        ValueError("Expected filename or list of lines but saw {schema_file} for schema file".format(**locals()))
    return [l for l in re.sub(r'\s*--.*|\n\s*\n', '', schema_str).split(';') if not re.match(r'^\s*$', l)]

class tmp_dir:
    def __enter__(self):
        self.directory = tempfile.mkdtemp()
        return self.directory

    def __exit__(self, type, value, traceback):
        shutil.rmtree(self.directory)

def rows_to_dsv(rows, delim="\t"):
    s = StringIO()
    writer = csv.writer(s, delimiter=delim)
    for row in rows:
        writer.writerow(row)
    return s.read()

def create_files(files, directory=None):
    for filename, contents in files.items():
        f = open(os.path.join(directory, filename) if directory is not None else filename, 'w')
        if type(contents) == str:
            f.write(contents)
        elif type(contents) == list:
            f.writelines(contents)
        else:
            f.write(contents)
        f.close()

# 
# class TemporaryFileMixin(object):
#     def tmp_csv(basename, lines):
