import pytest
from airflow_provider_sap_hana.hooks.hana import SapHanaHook
from airflow.models.connection import Connection
from hdbcli.resultrow import ResultRow
import json
import datetime
from unittest import mock


@pytest.fixture
def db_hook():
    def _db_hook(schema_override=False):
        connection = Connection(
            conn_type="hana", host="hanahost", login="user", password="pass123", port=12345, schema="hana_schema")
        hook = SapHanaHook(schema="schema_override" if schema_override else None)
        hook.get_connection = mock.Mock(return_value=connection)
        return hook
    return _db_hook


class TestSapHanaHookConn:
    @pytest.mark.parametrize(
        "schema_override, expected_uri",
        [
            (False, "hana://user:pass123@hanahost:12345/hana_schema"),
            (True, "hana://user:pass123@hanahost:12345/schema_override")
        ])
    def test_get_uri(self, schema_override, expected_uri, db_hook):
        hook = db_hook(schema_override)
        uri = hook.get_uri()
        assert uri == expected_uri

    @pytest.mark.parametrize(
        "schema_override, expected_sa_url",
        [
            (False, "hana+hdbcli://user:pass123@hanahost:12345/hana_schema"),
            (True, "hana+hdbcli://user:pass123@hanahost:12345/schema_override")
    ])
    def test_sqlalchemy_url(self, schema_override, expected_sa_url, db_hook):
        hook = db_hook(schema_override)
        sa_url = hook.sqlalchemy_url
        assert str(sa_url) == expected_sa_url

    @mock.patch("airflow_provider_sap_hana.hooks.hana.hdbcli.dbapi.connect")
    def test_get_conn(self, mock_connect, db_hook):
        hook = db_hook()
        hook.get_conn()
        mock_connect.assert_called_once_with(
            address="hanahost", user="user", password="pass123", port=12345, database="hana_schema")

    @pytest.mark.parametrize("is_autocommit_set, expected", [(True, True), (False, False)])
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
            year=1970, month=1, day=1, hour=0, minute=0, second=0, microsecond=123456)
        epoch_ts_add_day = epoch_ts + datetime.timedelta(days=1)
        epoch_ts_add_week = epoch_ts + datetime.timedelta(days=7)
        column_names = ("MOCK_STRING", "MOCK_INT", "MOCK_FLOAT", "MOCK_DATETIME", "MOCK_NONE")
        yield [
            ResultRow(
                column_names=column_names,
                column_values=("test123", 123, 123.00, epoch_ts, None)),
            ResultRow(
                column_names=column_names,
                column_values=("test456", 456, 456.00, epoch_ts_add_day, None)),
            ResultRow(
                column_names=column_names,
                column_values=("test789", 789, 789.00, epoch_ts_add_week, None))
            ]

    @pytest.fixture
    def cursor(self, resultrows):
        cur = mock.MagicMock(rowcount=-1)
        cur.fetchone = mock.Mock(return_value=resultrows[0])
        cur.fetchall = mock.Mock(return_value=resultrows)
        yield cur

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
        (4, type(None))
    ])
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
        ("fetchall", [
            ("test123", 123, 123.00, "1970-01-01T00:00:00.123456", None),
            ("test456", 456, 456.00, "1970-01-02T00:00:00.123456", None),
            ("test789", 789, 789.00, "1970-01-08T00:00:00.123456", None)
        ])
    ])
    def test_make_common_data_structure(self, handler, expected_data_structure, cursor, db_hook):
        hook = db_hook()
        result = getattr(cursor, handler)()
        common_result = hook._make_common_data_structure(result)
        assert common_result == expected_data_structure

    @pytest.mark.parametrize("handler", ["fetchone", "fetchall"])
    def test_common_data_structure_is_serializable(self, handler, cursor, db_hook):
        hook = db_hook()
        result = getattr(cursor, handler)()
        common_result = hook._make_common_data_structure(result)
        json.dumps(common_result)
