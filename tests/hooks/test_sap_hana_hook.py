from __future__ import annotations

import json
from unittest import mock

import pytest
from airflow.exceptions import AirflowException
from hdbcli.dbapi import ProgrammingError
from sqlalchemy_hana.dialect import RESERVED_WORDS


class TestSapHanaHook:
    @pytest.mark.parametrize(
        "schema_override, expected_uri",
        [
            (True, "hana://user:pass123@hanahost:12345/schema_override"),
            (False, "hana://user:pass123@hanahost:12345/hana_schema"),
        ],
    )
    def test_get_uri(self, schema_override, expected_uri, mock_hook):
        hook = mock_hook(schema_override=schema_override)
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
    def test_uri_with_extra(self, extra, expected_uri, mock_hook):
        hook = mock_hook(extra=extra)
        uri = hook.get_uri()
        assert uri == expected_uri

    @pytest.mark.parametrize(
        "schema_override, expected_sa_url",
        [
            (False, "hana+hdbcli://user:pass123@hanahost:12345/hana_schema"),
            (True, "hana+hdbcli://user:pass123@hanahost:12345/schema_override"),
        ],
    )
    def test_sqlalchemy_url(self, schema_override, expected_sa_url, mock_hook):
        hook = mock_hook(schema_override=schema_override)
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
    def test_get_conn(self, mock_connect, schema_override, called_with_database, mock_hook):
        hook = mock_hook(schema_override=schema_override)
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
    def test_get_conn_with_extra(self, mock_connect, extra, called_with_extra, mock_hook):
        hook = mock_hook(extra=extra)
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
    def test_get_autocommit(self, mock_connect, is_autocommit_set, expected, mock_conn, mock_hook):
        hook = mock_hook()
        mock_connect.return_value = mock_conn
        mock_conn.getautocommit.return_value = is_autocommit_set
        conn = hook.get_conn()
        autocommit = hook.get_autocommit(conn)
        mock_conn.getautocommit.assert_called_once()
        assert autocommit == expected

    @pytest.mark.parametrize("autocommit", [True, False])
    @mock.patch("airflow_provider_sap_hana.hooks.hana.hdbcli.dbapi.connect")
    def test_set_autocommit(self, mock_connect, autocommit, mock_conn, mock_hook):
        hook = mock_hook()
        mock_connect.return_value = mock_conn
        mock_conn.setautocommit.return_value = autocommit
        conn = hook.get_conn()
        hook.set_autocommit(conn, autocommit)
        mock_conn.setautocommit.assert_called_once_with(autocommit)

    def test_dialect_name_is_hana(self, mock_hook):
        hook = mock_hook()
        assert hook.dialect_name == "hana"

    @mock.patch("airflow_provider_sap_hana.hooks.hana.import_string")
    def test_get_reserved_words_import_dialect(self, mock_import, mock_hook):
        hook = mock_hook()
        hook.get_reserved_words(hook.dialect_name)
        mock_import.assert_called_once_with("sqlalchemy_hana.dialect")

    def test_reserved_words_equal_sa_hana_reserved_words(self, mock_hook):
        hook = mock_hook()
        assert hook.reserved_words == RESERVED_WORDS


class TestSapHanaResultRowSerialization:
    def test_resultrow_not_serializable(self, mock_cursor):
        result = mock_cursor.fetchone()
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
    def test_make_resultrow_cell_serializable(self, result_index, expected_type, mock_cursor, mock_hook):
        hook = mock_hook()
        result = mock_cursor.fetchone()
        cell = result[result_index]
        serialized_cell = hook._make_resultrow_cell_serializable(cell)
        assert isinstance(serialized_cell, expected_type)

    def test_make_resultrow_common(self, mock_cursor, mock_hook):
        hook = mock_hook()
        result = mock_cursor.fetchone()
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
    def test_make_common_data_structure(self, handler, expected_data_structure, mock_cursor, mock_hook):
        hook = mock_hook()
        result = getattr(mock_cursor, handler)()
        common_result = hook._make_common_data_structure(result)
        assert common_result == expected_data_structure

    @pytest.mark.parametrize(
        "handler, empty_result", [("fetchone", None), ("fetchall", []), ("fetchall", None)]
    )
    @mock.patch("airflow_provider_sap_hana.hooks.hana.SapHanaHook._make_resultrow_common")
    def test_make_common_data_structure_empty_result(
        self, mock_make_resultrow_common, handler, empty_result, mock_cursor, mock_hook
    ):
        hook = mock_hook()
        getattr(mock_cursor, handler).side_effect = lambda: empty_result
        result = getattr(mock_cursor, handler)()
        hook._make_common_data_structure(result)
        mock_make_resultrow_common.assert_not_called()

    @pytest.mark.parametrize("handler", ["fetchone", "fetchall"])
    def test_common_data_structure_is_serializable(self, handler, mock_cursor, mock_hook):
        hook = mock_hook()
        result = getattr(mock_cursor, handler)()
        common_result = hook._make_common_data_structure(result)
        json.dumps(common_result)


class TestSapHanaStreamRecords:
    @mock.patch("airflow_provider_sap_hana.hooks.hana.hdbcli.dbapi.connect")
    def test_stream_rows_fetchone_not_called_until_next_called_on_generator(
        self, mock_connect, mock_conn, mock_cursor, mock_hook
    ):
        hook = mock_hook()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        results = hook._stream_records(mock_conn, mock_cursor)
        mock_cursor.fetchone.assert_not_called()
        next(results)
        mock_cursor.fetchone.assert_called_once()
        list(results)
        assert mock_cursor.fetchone.call_count == 4

    @mock.patch("airflow_provider_sap_hana.hooks.hana.hdbcli.dbapi.connect")
    def test_stream_rows_resources_closed_when_cursor_exhausted(
        self, mock_connect, mock_conn, mock_cursor, mock_hook
    ):
        hook = mock_hook()
        mock_connect.return_value = mock_conn

        results = hook._stream_records(mock_conn, mock_cursor)
        list(results)
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @pytest.mark.parametrize(
        "exception, message",
        [
            (AirflowException, "Something wrong with Airflow!"),
            (ProgrammingError, "Something wrong with HANA!"),
            (SystemExit, "Lots of things going wrong!"),
        ],
    )
    @mock.patch("airflow_provider_sap_hana.hooks.hana.hdbcli.dbapi.connect")
    def test_stream_rows_resources_closed_on_exception(
        self, mock_connect, exception, message, mock_conn, mock_cursor, mock_hook
    ):
        hook = mock_hook()
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.side_effect = exception(message)
        with pytest.raises(exception):
            results = hook._stream_records("SELECT mock FROM dummy", mock_cursor)
            next(results)
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @mock.patch("airflow_provider_sap_hana.hooks.hana.hdbcli.dbapi.connect")
    def test_cursor_description_is_available_immediately(
        self, mock_connect, mock_conn, mock_cursor, mock_hook
    ):
        hook = mock_hook()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        hook._stream_records(mock_connect, mock_cursor)
        expected_last_description = (
            ("MOCK_STRING",),
            ("MOCK_INT",),
            ("MOCK_FLOAT",),
            ("MOCK_DATETIME",),
            ("MOCK_NONE",),
        )
        assert hook.last_description == expected_last_description
        mock_cursor.fetchone.assert_not_called()
        mock_cursor.close.assert_not_called()
        mock_conn.close.assert_not_called()

    @mock.patch("airflow_provider_sap_hana.hooks.hana.hdbcli.dbapi.connect")
    def test_make_cursor_description_available_immediately_resources_closed_on_exception(
        self, mock_connect, mock_conn, mock_cursor, mock_hook
    ):
        hook = mock_hook()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = ProgrammingError("Bad SQL statement")
        with pytest.raises(ProgrammingError):
            hook.stream_records("SELECT mock FROM dummy")

        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()


class TestSapHanaHookBulkInsertRows:
    def test_simpler_serialize_cells_result_equals_dbapihook_insert_rows_serialize_cells(
        self, mock_insert_values, mock_hook
    ):
        hook = mock_hook()

        dbapihook_insert_rows_values_call = list(
            map(
                lambda row: hook._serialize_cells(row, None),
                mock_insert_values,
            )
        )
        sap_hana_hook_bulk_insert_call = list(map(hook._serialize_cells, mock_insert_values))
        assert dbapihook_insert_rows_values_call == sap_hana_hook_bulk_insert_call

    @mock.patch("airflow_provider_sap_hana.hooks.hana.hdbcli.dbapi.connect")
    def test_prepare_cursor(
        self,
        mock_connect,
        mock_conn,
        mock_dml_cursor,
        mock_hook,
        mock_insert_values,
    ):
        hook = mock_hook()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_dml_cursor

        expected_sql = hook._generate_insert_sql("mock", mock_insert_values[0], ["mock_col1", "mock_col2"])
        hook.bulk_insert_rows(table="mock", rows=mock_insert_values, target_fields=["mock_col1", "mock_col2"])
        mock_dml_cursor.prepare.assert_called_once_with(expected_sql, newcursor=False)

    @pytest.mark.parametrize("commit_every, expected_call_count", [(0, 1), (5, 4), (10, 2), (15, 2)])
    @mock.patch("airflow_provider_sap_hana.hooks.hana.hdbcli.dbapi.connect")
    def test_bulk_insert_rows_batches(
        self,
        mock_connect,
        commit_every,
        expected_call_count,
        mock_conn,
        mock_dml_cursor,
        mock_insert_values,
        mock_hook,
    ):
        hook = mock_hook()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_dml_cursor

        hook.bulk_insert_rows(table="mock", rows=mock_insert_values, commit_every=commit_every)
        assert mock_dml_cursor.executemanyprepared.call_count == expected_call_count

    @pytest.mark.parametrize(
        "autocommit, commit_every, expected_call_count",
        [(True, 0, 0), (False, 0, 1), (True, 5, 0), (False, 5, 4)],
    )
    @mock.patch("airflow_provider_sap_hana.hooks.hana.hdbcli.dbapi.connect")
    def test_bulk_insert_rows_autocommit(
        self,
        mock_connect,
        autocommit,
        commit_every,
        expected_call_count,
        mock_conn,
        mock_insert_values,
        mock_hook,
    ):
        hook = mock_hook()
        mock_connect.return_value = mock_conn
        hook.bulk_insert_rows(
            table="mock", rows=mock_insert_values, commit_every=commit_every, autocommit=autocommit
        )
        assert mock_conn.commit.call_count == expected_call_count

    @pytest.mark.parametrize(
        "commit_every, expected_rowcount", [(0, None), (5, [5, 10, 15, 20]), (10, [10, 20]), (15, [15, 20])]
    )
    @mock.patch("airflow_provider_sap_hana.hooks.hana.hdbcli.dbapi.connect")
    def test_bulk_insert_rows_rowcount_logging(
        self,
        mock_connect,
        commit_every,
        expected_rowcount,
        mock_conn,
        mock_dml_cursor,
        mock_insert_values,
        mock_hook,
        caplog,
    ):
        hook = mock_hook()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_dml_cursor
        hook.bulk_insert_rows(
            table="mock",
            rows=mock_insert_values,
            commit_every=commit_every,
            target_fields=["mock_col1", "mock_col2"],
        )
        assert "Prepared statement: INSERT INTO mock (mock_col1, mock_col2) VALUES (?,?)" in caplog.messages
        if expected_rowcount:
            for call in expected_rowcount:
                assert f"Loaded {call} rows into mock so far" in caplog.messages
        assert "Done loading. Loaded a total of 20 rows into mock" in caplog.messages
