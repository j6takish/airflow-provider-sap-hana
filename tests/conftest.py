from __future__ import annotations

import datetime
from unittest import mock

import pytest
from airflow.models.connection import Connection
from hdbcli.dbapi import Connection as HDBCLIConnection, Cursor
from hdbcli.resultrow import ResultRow

from airflow_provider_sap_hana.hooks.hana import SapHanaHook


@pytest.fixture
def mock_hook():
    def _mock_hook(schema_override=False, extra=None):
        connection = Connection(
            conn_type="hana",
            host="hanahost",
            login="user",
            password="pass123",
            port=12345,
            schema="hana_schema",
        )
        if extra:
            connection.extra = extra
        hook = SapHanaHook(schema="schema_override" if schema_override else None)
        hook.get_connection = mock.Mock(return_value=connection)
        return hook

    return _mock_hook


@pytest.fixture
def mock_resultrows():
    epoch_ts = datetime.datetime(year=1970, month=1, day=1, hour=0, minute=0, second=0, microsecond=123456)
    epoch_ts_add_day = epoch_ts + datetime.timedelta(days=1)
    epoch_ts_add_week = epoch_ts + datetime.timedelta(days=7)
    column_names = ("MOCK_STRING", "MOCK_INT", "MOCK_FLOAT", "MOCK_DATETIME", "MOCK_NONE")
    return [
        ResultRow(column_names=column_names, column_values=("test123", 123, 123.00, epoch_ts, None)),
        ResultRow(column_names=column_names, column_values=("test456", 456, 456.00, epoch_ts_add_day, None)),
        ResultRow(column_names=column_names, column_values=("test789", 789, 789.00, epoch_ts_add_week, None)),
    ]


@pytest.fixture
def mock_insert_values():
    data = []
    for _ in range(20):
        data.append(("mock1", "mock2"))
    return data


@pytest.fixture
def mock_cursor(mock_resultrows):
    result_iterator = iter(mock_resultrows)
    cur = mock.MagicMock(autospec=Cursor, rowcount=-1)
    cur.__iter__.return_value = result_iterator
    cur.fetchone.side_effect = lambda: next(result_iterator, None)
    cur.fetchall.side_effect = lambda: list(result_iterator)
    cur.description = (
        ("MOCK_STRING",),
        ("MOCK_INT",),
        ("MOCK_FLOAT",),
        ("MOCK_DATETIME",),
        ("MOCK_NONE",),
    )
    return cur


@pytest.fixture
def mock_conn(mock_cursor):
    conn = mock.MagicMock(autospec=HDBCLIConnection)
    conn.cursor.return_value = mock_cursor
    mock_cursor.connection = conn
    return conn


@pytest.fixture
def mock_dml_cursor():
    cur = mock.MagicMock(autospec=Cursor, rowcount=-1)
    cur.executemany.side_effect = lambda sql, values: setattr(cur, "rowcount", len(values))
    cur.executemanyprepared.side_effect = lambda values: setattr(cur, "rowcount", len(values))
    cur.description = None
    return cur
