# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from unittest import mock
import unittest

from airflow.hooks.dbapi_hook import DbApiHook


class TestDbApiHook(unittest.TestCase):

    def setUp(self):
        super().setUp()

        self.cur = mock.MagicMock()
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestDbApiHook(DbApiHook):
            conn_name_attr = 'test_conn_id'

            def get_conn(self):
                return conn

        self.db_hook = UnitTestDbApiHook()

    def test_get_records(self):
        statement = "SQL"
        rows = [("hello",),
                ("world",)]

        self.cur.fetchall.return_value = rows

        self.assertEqual(rows, self.db_hook.get_records(statement))

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1
        self.cur.execute.assert_called_once_with(statement)

    def test_get_records_parameters(self):
        statement = "SQL"
        parameters = ["X", "Y", "Z"]
        rows = [("hello",),
                ("world",)]

        self.cur.fetchall.return_value = rows

        self.assertEqual(rows, self.db_hook.get_records(statement, parameters))

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1
        self.cur.execute.assert_called_once_with(statement, parameters)

    def test_get_records_exception(self):
        statement = "SQL"
        self.cur.fetchall.side_effect = RuntimeError('Great Problems')

        with self.assertRaises(RuntimeError):
            self.db_hook.get_records(statement)

        self.conn.close.call_count == 1
        self.cur.close.call_count == 1
        self.cur.execute.assert_called_once_with(statement)

    def test_insert_rows(self):
        table = "table"
        rows = [("hello",),
                ("world",)]

        self.db_hook.insert_rows(table, rows)

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1

        commit_count = 2  # The first and last commit
        self.assertEqual(commit_count, self.conn.commit.call_count)

        sql = "INSERT INTO {}  VALUES (%s)".format(table)
        for row in rows:
            self.cur.execute.assert_any_call(sql, row)

    def test_bulk_insert(self):
        # bulk_insert default case, without passing custom numbers for
        # commit_every, insert_every
        table = "table"
        rows = [(1, 2)] * 22

        self.db_hook.bulk_insert(table, rows)

        self.conn.close.assert_called_once()
        self.cur.close.assert_called_once()

        commit_count = 2  # Including the first commit
        execute_count = 1  # All the records should be executed once.
        self.assertEqual(commit_count, self.conn.commit.call_count)
        self.assertEqual(execute_count, self.cur.execute.call_count)

    def test_bulk_insert_to_commit_rate_case1(self):
        # Case-1: Committing more records than inserting records.
        table = "table"
        rows = [(1, 2)] * 22
        insert_to_commit_rate, insert_every = 2, 5

        self.db_hook.bulk_insert(
            table,
            rows,
            insert_to_commit_rate=insert_to_commit_rate,
            insert_every=insert_every)

        self.conn.close.assert_called_once()
        self.cur.close.assert_called_once()

        commit_count = 4  # Including the first commit
        self.assertEqual(commit_count, self.conn.commit.call_count)

    def test_bulk_insert_to_commit_rate_case2(self):
        # Case-2: Committing all the records at once.
        table = "table"
        rows = [(1, 2)] * 22

        # Committing few records each time
        insert_to_commit_rate = 1

        self.db_hook.bulk_insert(
            table,
            rows,
            insert_to_commit_rate=insert_to_commit_rate)

        self.conn.close.assert_called_once()
        self.cur.close.assert_called_once()

        commit_count = 2  # Including the first commit
        self.assertEqual(commit_count, self.conn.commit.call_count)

    def test_bulk_insert_insert_every_case1(self):
        # Case-1: Committing less records than total records.
        table = "table"
        rows = [(1, 2)] * 22
        insert_every = 5

        self.db_hook.bulk_insert(
            table,
            rows,
            insert_every=insert_every)

        self.conn.close.assert_called_once()
        self.cur.close.assert_called_once()

        execute_count = 5  # 22 records -> 5, 5, 5, 5, 2 -> total 5 executes
        self.assertEqual(execute_count, self.cur.execute.call_count)

    def test_bulk_insert_insert_every_case2(self):
        # Case-2: Inserting all the records at once
        table = "table"
        rows = [(1, 2)] * 22
        insert_every = 30

        self.db_hook.bulk_insert(
            table,
            rows,
            insert_every=insert_every)

        self.conn.close.assert_called_once()
        self.cur.close.assert_called_once()

        execute_count = 1
        self.assertEqual(execute_count, self.cur.execute.call_count)

    def test_bulk_insert_insert_every_case3(self):
        # Case-3: Inserting single row once
        table = "table"
        rows = [(1, 2)] * 22
        insert_every = 1

        self.db_hook.bulk_insert(
            table,
            rows,
            insert_every=insert_every)

        self.conn.close.assert_called_once()
        self.cur.close.assert_called_once()

        execute_count, commit_count = 22, 12
        self.assertEqual(execute_count, self.cur.execute.call_count)
        self.assertEqual(commit_count, self.conn.commit.call_count)

    def test_bulk_insert_target_fields(self):
        table = "table"
        rows = [(1, 2)] * 22
        target_fields = ["field1", "field2"]

        # committing everything at once
        insert_to_commit_rate, insert_every = 1, 30

        self.db_hook.bulk_insert(
            table,
            rows,
            target_fields=target_fields,
            insert_to_commit_rate=insert_to_commit_rate,
            insert_every=insert_every)

        self.conn.close.assert_called_once()
        self.cur.close.assert_called_once()

        commit_count = 2  # The first and last commit
        execute_count = 1  # All the records should be executed once.
        self.assertEqual(commit_count, self.conn.commit.call_count)
        self.assertEqual(execute_count, self.cur.execute.call_count)

    def test_insert_rows_replace(self):
        table = "table"
        rows = [("hello",),
                ("world",)]

        self.db_hook.insert_rows(table, rows, replace=True)

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1

        commit_count = 2  # The first and last commit
        self.assertEqual(commit_count, self.conn.commit.call_count)

        sql = "REPLACE INTO {}  VALUES (%s)".format(table)
        for row in rows:
            self.cur.execute.assert_any_call(sql, row)

    def test_insert_rows_target_fields(self):
        table = "table"
        rows = [("hello",),
                ("world",)]
        target_fields = ["field"]

        self.db_hook.insert_rows(table, rows, target_fields)

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1

        commit_count = 2  # The first and last commit
        self.assertEqual(commit_count, self.conn.commit.call_count)

        sql = "INSERT INTO {} ({}) VALUES (%s)".format(table, target_fields[0])
        for row in rows:
            self.cur.execute.assert_any_call(sql, row)

    def test_insert_rows_commit_every(self):
        table = "table"
        rows = [("hello",),
                ("world",)]
        commit_every = 1

        self.db_hook.insert_rows(table, rows, commit_every=commit_every)

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1

        commit_count = 2 + divmod(len(rows), commit_every)[0]
        self.assertEqual(commit_count, self.conn.commit.call_count)

        sql = "INSERT INTO {}  VALUES (%s)".format(table)
        for row in rows:
            self.cur.execute.assert_any_call(sql, row)
