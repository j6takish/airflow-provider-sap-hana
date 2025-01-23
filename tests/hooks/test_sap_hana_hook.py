from __future__ import annotations

import datetime
import json
from unittest import mock

import pytest
from airflow.models.connection import Connection
from hdbcli.resultrow import ResultRow

from airflow_provider_sap_hana.hooks.hana import SapHanaHook


@pytest.fixture
def db_hook():
    def _db_hook(schema_override=False, extra=None):
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

    return _db_hook


class TestSapHanaHookConn:
    @pytest.mark.parametrize(
        "schema_override, expected_uri",
        [
            (True, "hana://user:pass123@hanahost:12345/schema_override"),
            (False, "hana://user:pass123@hanahost:12345/hana_schema"),
        ],
    )
    def test_get_uri(self, schema_override, expected_uri, db_hook):
        hook = db_hook(schema_override=schema_override)
        uri = hook.get_uri()
        assert uri == expected_uri

    @pytest.mark.parametrize(
        "extra, expected_uri",
        [
            (
                '{"nodeConnectTimeout": "1000"}',
                "hana://user:pass123@hanahost:12345/hana_schema?nodeConnectTimeout=1000",
            ),
            (
                '{"packetSizeLimit": "1073741823"}',
                "hana://user:pass123@hanahost:12345/hana_schema?packetSizeLimit=1073741823",
            ),
            (
                '{"prefetch": "true", "cursorHoldabilityType": "rollback"}',
                "hana://user:pass123@hanahost:12345/hana_schema?prefetch=true&cursorHoldabilityType=rollback",
            ),
        ],
    )
    def test_uri_with_extra(self, extra, expected_uri, db_hook):
        hook = db_hook(extra=extra)
        uri = hook.get_uri()
        assert uri == expected_uri

    @pytest.mark.parametrize(
        "schema_override, expected_sa_url",
        [
            (False, "hana+hdbcli://user:pass123@hanahost:12345/hana_schema"),
            (True, "hana+hdbcli://user:pass123@hanahost:12345/schema_override"),
        ],
    )
    def test_sqlalchemy_url(self, schema_override, expected_sa_url, db_hook):
        hook = db_hook(schema_override=schema_override)
        sa_url = hook.sqlalchemy_url
        assert str(sa_url) == expected_sa_url

    @pytest.mark.parametrize(
        "schema_override, called_with_database",
        [
            (False, "hana_schema"),
            (True, "schema_override"),
        ],
    )
    @mock.patch("airflow_provider_sap_hana.hooks.hana.hdbcli.dbapi.connect")
    def test_get_conn(self, mock_connect, schema_override, called_with_database, db_hook):
        hook = db_hook(schema_override=schema_override)
        hook.get_conn()
        mock_connect.assert_called_once_with(
            address="hanahost", user="user", password="pass123", port=12345, databasename=called_with_database
        )

    @pytest.mark.parametrize(
        "extra, called_with_extra",
        [
            ('{"nodeConnectTimeout": "1000"}', {"nodeConnectTimeout": "1000"}),
            ('{"packetSizeLimit": "1073741823"}', {"packetSizeLimit": "1073741823"}),
            (
                '{"prefetch": "true", "cursorHoldabilityType": "rollback"}',
                {"prefetch": "true", "cursorHoldabilityType": "rollback"},
            ),
        ],
    )
    @mock.patch("airflow_provider_sap_hana.hooks.hana.hdbcli.dbapi.connect")
    def test_get_conn_with_extra(self, mock_connect, extra, called_with_extra, db_hook):
        hook = db_hook(extra=extra)
        hook.get_conn()
        mock_connect.assert_called_once_with(
            address="hanahost",
            user="user",
            password="pass123",
            port=12345,
            databasename="hana_schema",
            **called_with_extra,
        )

    @pytest.mark.parametrize(
        "is_autocommit_set, expected",
        [
            (True, True),
            (False, False),
        ],
    )
    @mock.patch("airflow_provider_sap_hana.hooks.hana.hdbcli.dbapi.connect")
    def test_get_autocommit(self, mock_connect, is_autocommit_set, expected, db_hook):
        mock_getautocommit = mock_connect.return_value.getautocommit
        mock_getautocommit.return_value = is_autocommit_set
        hook = db_hook()
        conn = hook.get_conn()
        autocommit = hook.get_autocommit(conn)
        mock_getautocommit.assert_called_once()
        assert autocommit == expected

    @pytest.mark.parametrize("autocommit", [True, False])
    @mock.patch("airflow_provider_sap_hana.hooks.hana.hdbcli.dbapi.connect")
    def test_set_autocommit(self, mock_connect, autocommit, db_hook):
        mock_setautocommit = mock_connect.return_value.setautocommit
        hook = db_hook()
        conn = hook.get_conn()
        hook.set_autocommit(conn, autocommit)
        mock_setautocommit.assert_called_once_with(autocommit)


class TestSapHanaHook:
    @pytest.fixture
    def resultrows(self):
        epoch_ts = datetime.datetime(
            year=1970, month=1, day=1, hour=0, minute=0, second=0, microsecond=123456
        )
        epoch_ts_add_day = epoch_ts + datetime.timedelta(days=1)
        epoch_ts_add_week = epoch_ts + datetime.timedelta(days=7)
        column_names = ("MOCK_STRING", "MOCK_INT", "MOCK_FLOAT", "MOCK_DATETIME", "MOCK_NONE")
        return [
            ResultRow(column_names=column_names, column_values=("test123", 123, 123.00, epoch_ts, None)),
            ResultRow(
                column_names=column_names, column_values=("test456", 456, 456.00, epoch_ts_add_day, None)
            ),
            ResultRow(
                column_names=column_names, column_values=("test789", 789, 789.00, epoch_ts_add_week, None)
            ),
        ]

    @pytest.fixture
    def cursor(self, resultrows):
        cur = mock.MagicMock(rowcount=-1)
        cur.fetchone = mock.Mock(return_value=resultrows[0])
        cur.fetchall = mock.Mock(return_value=resultrows)
        return cur

    @pytest.fixture
    def conn(self, cursor):
        conn = mock.Mock()
        conn.cursor.return_value = cursor
        return conn

    def test_resultrow_not_serializable(self, cursor):
        result = cursor.fetchone()
        with pytest.raises(TypeError, match="not JSON serializable"):
            json.dumps(result)

    @pytest.mark.parametrize(
        "result_index, expected_type",
        [
            (0, str),
            (1, int),
            (2, float),
            (3, str),
            (4, type(None)),
        ],
    )
    def test_make_resultrow_cell_serializable(self, result_index, expected_type, cursor, db_hook):
        hook = db_hook()
        result = cursor.fetchone()
        cell = result[result_index]
        serialized_cell = hook._make_resultrow_cell_serializable(cell)
        assert isinstance(serialized_cell, expected_type)

    def test_make_resultrow_common(self, cursor, db_hook):
        hook = db_hook()
        result = cursor.fetchone()
        common_result = hook._make_resultrow_common(result)
        expected_result = ("test123", 123, 123.00, "1970-01-01T00:00:00.123456", None)
        assert common_result == expected_result

    @pytest.mark.parametrize(
        "handler, expected_data_structure",
        [
            ("fetchone", ("test123", 123, 123.00, "1970-01-01T00:00:00.123456", None)),
            (
                "fetchall",
                [
                    ("test123", 123, 123.00, "1970-01-01T00:00:00.123456", None),
                    ("test456", 456, 456.00, "1970-01-02T00:00:00.123456", None),
                    ("test789", 789, 789.00, "1970-01-08T00:00:00.123456", None),
                ],
            ),
        ],
    )
    def test_make_common_data_structure(self, handler, expected_data_structure, cursor, db_hook):
        hook = db_hook()
        result = getattr(cursor, handler)()
        common_result = hook._make_common_data_structure(result)
        assert common_result == expected_data_structure

    @pytest.mark.parametrize(
        "handler, empty_result", [("fetchone", None), ("fetchall", []), ("fetchall", None)]
    )
    @mock.patch("airflow_provider_sap_hana.hooks.hana.SapHanaHook._make_resultrow_common")
    def test_make_common_data_structure_empty_result(
        self, mock_make_resultrow_common, handler, empty_result, cursor, db_hook
    ):
        hook = db_hook()
        getattr(cursor, handler).return_value = empty_result
        result = getattr(cursor, handler)()
        hook._make_common_data_structure(result)
        mock_make_resultrow_common.assert_not_called()

    @pytest.mark.parametrize("handler", ["fetchone", "fetchall"])
    def test_common_data_structure_is_serializable(self, handler, cursor, db_hook):
        hook = db_hook()
        result = getattr(cursor, handler)()
        common_result = hook._make_common_data_structure(result)
        json.dumps(common_result)

    @pytest.mark.parametrize(
        "get_records_return_val, expected_pks",
        [
            ([("TEST_COLUMN",)], ["TEST_COLUMN"]),
            ([("TEST_COLUMN",), ("TEST_COLUMN2",)], ["TEST_COLUMN", "TEST_COLUMN2"]),
            (None, None),
        ],
    )
    @mock.patch("airflow_provider_sap_hana.hooks.hana.DbApiHook.get_records")
    def test_table_get_primary_key(self, mock_get_records, get_records_return_val, expected_pks, db_hook):
        hook = db_hook()
        table = "SYS.TEST_TABLE"
        sql = """
        SELECT column_name
        FROM SYS.CONSTRAINTS
        WHERE
            is_primary_key = 'TRUE'
            AND schema_name = ?
            AND table_name = ?
        """
        mock_get_records.return_value = get_records_return_val
        pks = hook.get_table_primary_key(table)
        mock_get_records.assert_called_once_with(sql=sql, parameters=("SYS", "TEST_TABLE"))
        assert pks == expected_pks
