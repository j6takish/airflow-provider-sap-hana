from __future__ import annotations

import json
import warnings
from collections.abc import Iterator
from unittest import mock

import importlib_metadata as md
import pytest
from hdbcli.dbapi import ProgrammingError
from sqlalchemy_hana.dialect import RESERVED_WORDS

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.common.compat.version_compat import get_base_airflow_version_tuple
from airflow_provider_sap_hana.hooks.hana import SapHanaHook


class TestSapHanaHookConnection:
    @pytest.mark.parametrize(
        "hook_database, connection_database, expected_sqlalchemy_url",
        [
            ("hook_database", None, "hana+hdbcli://user:pass123@hanahost:12345/hook_database"),
            (None, "connection_database", "hana+hdbcli://user:pass123@hanahost:12345/connection_database"),
            (None, None, "hana+hdbcli://user:pass123@hanahost:12345"),
        ],
    )
    def test_sqlalchemy_url(
        self, hook_database, connection_database, expected_sqlalchemy_url, mock_connection, is_sqlalchemy_v2
    ):
        mock_connection.schema = connection_database
        hook = SapHanaHook(database=hook_database)
        hook.get_connection = mock.Mock(return_value=mock_connection)
        if is_sqlalchemy_v2:
            assert hook.sqlalchemy_url.render_as_string(False) == expected_sqlalchemy_url
        else:
            assert str(hook.sqlalchemy_url) == expected_sqlalchemy_url

    @pytest.mark.parametrize(
        "extra, expected_sqlalchemy_url",
        [
            (
                '{"nodeconnecttimeout": "1000"}',
                "hana+hdbcli://user:pass123@hanahost:12345?nodeconnecttimeout=1000",
            ),
            (
                '{"packetsizelimit": "1073741823"}',
                "hana+hdbcli://user:pass123@hanahost:12345?packetsizelimit=1073741823",
            ),
            (
                '{"prefetch": "true", "cursorholdabilitytype": "rollback"}',
                "hana+hdbcli://user:pass123@hanahost:12345?cursorholdabilitytype=rollback&prefetch=true",
            ),
            (
                '{"databasename": "mock", "chopblanks": "true", "chopblanksinput": "true"}',
                "hana+hdbcli://user:pass123@hanahost:12345?chopblanks=true&chopblanksinput=true",
            ),
        ],
    )
    def test_sqlalchemy_url_with_extra(
        self, extra, expected_sqlalchemy_url, mock_connection, is_sqlalchemy_v2
    ):
        mock_connection.extra = extra
        hook = SapHanaHook()
        hook.get_connection = mock.Mock(return_value=mock_connection)
        if is_sqlalchemy_v2:
            assert hook.sqlalchemy_url.render_as_string(False) == expected_sqlalchemy_url
        else:
            assert str(hook.sqlalchemy_url) == expected_sqlalchemy_url

    def test_get_uri(self, mock_hook):
        uri = mock_hook.get_uri()
        assert uri == "hana+hdbcli://user:***@hanahost:12345"

    @pytest.mark.parametrize(
        "replace_with_primary_key, expected_replace_stmt_format, expected_replace_stmt",
        [
            (
                True,
                "UPSERT {} {} VALUES ({}) WITH PRIMARY KEY",
                "UPSERT mock.mock  VALUES (?,?,?) WITH PRIMARY KEY",
            ),
            (False, "UPSERT {} {} VALUES ({})", "UPSERT mock.mock  VALUES (?,?,?)"),
        ],
    )
    def test_replace_statement_hook_param(
        self, replace_with_primary_key, expected_replace_stmt_format, expected_replace_stmt, mock_connection
    ):
        hook = SapHanaHook(replace_with_primary_key=replace_with_primary_key)
        hook.get_connection = mock.Mock(return_value=mock_connection)
        assert hook.replace_statement_format == expected_replace_stmt_format

        replace_stmt = hook._generate_insert_sql(
            table="mock.mock", values=["mock", "mock", "mock"], replace=True
        )
        assert replace_stmt == expected_replace_stmt

    @pytest.mark.parametrize(
        "extra, called_with_args",
        [
            (
                '{"databasename": "mock", "chopblanks": "true", "chopblanksinput": "true"}',
                {
                    "address": "hanahost",
                    "user": "user",
                    "password": "pass123",
                    "port": 12345,
                    "chopblanks": "true",
                    "chopblanksinput": "true",
                },
            ),
            (
                None,
                {
                    "address": "hanahost",
                    "user": "user",
                    "password": "pass123",
                    "port": 12345,
                },
            ),
        ],
    )
    @mock.patch("airflow_provider_sap_hana.hooks.hana.hdbcli.dbapi.connect")
    def test_get_conn(self, mock_connect, extra, called_with_args, mock_connection):
        mock_connection.extra = extra
        hook = SapHanaHook()
        hook.get_connection = mock.Mock(return_value=mock_connection)

        hook.get_conn()
        mock_connect.assert_called_once_with(**called_with_args)

    @pytest.mark.parametrize(
        "hook_database, connection_database, called_with_args",
        [
            (
                "hook_database",
                None,
                {
                    "address": "hanahost",
                    "user": "user",
                    "password": "pass123",
                    "port": 12345,
                    "databasename": "hook_database",
                },
            ),
            (
                None,
                "connection_database",
                {
                    "address": "hanahost",
                    "user": "user",
                    "password": "pass123",
                    "port": 12345,
                    "databasename": "connection_database",
                },
            ),
            (
                None,
                None,
                {
                    "address": "hanahost",
                    "user": "user",
                    "password": "pass123",
                    "port": 12345,
                },
            ),
        ],
    )
    @mock.patch("airflow_provider_sap_hana.hooks.hana.hdbcli.dbapi.connect")
    def test_get_conn_databasename(
        self, mock_connect, hook_database, connection_database, called_with_args, mock_connection
    ):
        mock_connection.schema = connection_database
        hook = SapHanaHook(database=hook_database)
        hook.get_connection = mock.Mock(return_value=mock_connection)

        hook.get_conn()
        mock_connect.assert_called_once_with(**called_with_args)

    @pytest.mark.parametrize(
        "enable_db_log_messages, extra, called_with_args",
        [
            (True, '{"traceOptions": "SQL=DEBUG,TIMING=ON"}', "SQL=DEBUG,TIMING=ON"),
            (False, '{"traceOptions": "SQL=DEBUG,TIMING=ON"}', ""),
            (True, None, "SQL=INFO,FLUSH=ON"),
        ],
    )
    @mock.patch("airflow_provider_sap_hana.hooks.hana.hdbcli.dbapi.connect")
    def test_get_conn_with_log_messaging(
        self, mock_connect, enable_db_log_messages, extra, called_with_args, mock_connection, mock_conn
    ):
        mock_connection.extra = extra
        hook = SapHanaHook(enable_db_log_messages=enable_db_log_messages)
        hook.get_connection = mock.Mock(return_value=mock_connection)
        mock_connect.return_value = mock_conn

        hook.get_conn()
        if not enable_db_log_messages:
            mock_conn.ontrace.assert_not_called()
        else:
            mock_conn.ontrace.assert_called_once_with(hook._log_message, called_with_args)

    # Test will be obsolete in next release
    @pytest.mark.airflow_deprecation_warning
    def test_replace_statement_backup_deprecation_warning(self, mock_hook):
        with pytest.warns(AirflowProviderDeprecationWarning):
            replace_stmt = mock_hook.replace_statement_format_backup
        assert replace_stmt == "UPSERT {} {} VALUES ({})"

    # Test will be obsolete in next release
    @pytest.mark.airflow_deprecation_warning
    def test_hook_schema_deprecation_warning(self):
        with pytest.warns(AirflowProviderDeprecationWarning):
            hook = SapHanaHook(schema="schema")
        assert hook.database == "schema"

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            hook = SapHanaHook(database="database")
        assert hook.database == "database"

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            hook = SapHanaHook()
        assert hook.database is None


class TestSapHanaHook:
    @pytest.mark.parametrize(
        "is_autocommit_set, expected",
        [
            (True, True),
            (False, False),
        ],
    )
    def test_get_autocommit(self, is_autocommit_set, expected, mock_conn, mock_hook):
        hook = mock_hook
        hook.get_conn = mock.Mock(return_value=mock_conn)
        mock_conn.getautocommit.return_value = is_autocommit_set

        autocommit = hook.get_autocommit(mock_conn)
        mock_conn.getautocommit.assert_called_once()
        assert autocommit == expected

    @pytest.mark.parametrize("autocommit", [True, False])
    def test_set_autocommit(self, autocommit, mock_conn, mock_hook):
        hook = mock_hook
        hook.get_conn = mock.Mock(return_value=mock_conn)
        mock_conn.setautocommit.return_value = autocommit

        hook.set_autocommit(mock_conn, autocommit)
        mock_conn.setautocommit.assert_called_once_with(autocommit)

    @mock.patch("airflow_provider_sap_hana.hooks.hana.import_string")
    def test_get_reserved_words_import_dialect(self, mock_import, mock_hook):
        hook = mock_hook

        hook.get_reserved_words(hook.dialect_name)
        mock_import.assert_called_once_with("sqlalchemy_hana.dialect")

    def test_reserved_words_equal_sa_hana_reserved_words(self, mock_hook):
        hook = mock_hook
        assert hook.reserved_words == RESERVED_WORDS

    # TODO Remove after logging issues resolved in airflow 3.1.0
    @pytest.mark.xfail(
        get_base_airflow_version_tuple() == (3, 1, 0), reason="Logging not working in airflow 3.1"
    )
    def test_db_log_messages(self, mock_conn, mock_dml_cursor, mock_hook, mock_insert_values, caplog):
        hdbcli_version = md.version("hdbcli")
        connect_message = f"libSQLDBCHDB {hdbcli_version}\nSYSTEM: Airflow\n"
        executemany_message = "::GET ROWS AFFECTED [0xmock00]\nROWS: 10"

        hook = mock_hook
        hook.enable_db_log_messages = True
        hook.get_conn = mock.Mock(return_value=mock_conn)

        mock_conn.side_effect = hook._log_message(connect_message)
        mock_conn.cursor.return_value = mock_dml_cursor
        mock_dml_cursor.executemany.side_effect = hook._log_message(executemany_message)

        with caplog.at_level(20):
            hook.bulk_insert_rows(table="mock", rows=[mock_insert_values()])
        hook.get_db_log_messages()

        # are they indented 4 spaces and is libSQLDBCHDB on a newline?
        expected_connect_message = f"\n    libSQLDBCHDB {hdbcli_version}\n    SYSTEM: Airflow\n"
        expected_executemany_message = "    ::GET ROWS AFFECTED [0xmock00]\n    ROWS: 10"

        assert expected_connect_message in caplog.text
        assert expected_executemany_message in caplog.text


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
        hook = mock_hook
        result = mock_cursor.fetchone()
        cell = result[result_index]
        serialized_cell = hook._make_resultrow_cell_serializable(cell)
        assert isinstance(serialized_cell, expected_type)

    def test_make_resultrow_common(self, mock_cursor, mock_hook):
        hook = mock_hook
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
        hook = mock_hook
        result = getattr(mock_cursor, handler)()
        common_result = hook._make_common_data_structure(result)
        assert common_result == expected_data_structure

    @pytest.mark.parametrize(
        "handler, empty_result", [("fetchone", None), ("fetchall", []), ("fetchall", None)]
    )
    def test_make_common_data_structure_empty_result(self, handler, empty_result, mock_cursor, mock_hook):
        hook = mock_hook
        mock_make_resultrow_common = mock.Mock()
        hook._make_resultrow_common = mock_make_resultrow_common

        getattr(mock_cursor, handler).side_effect = lambda: empty_result
        result = getattr(mock_cursor, handler)()
        hook._make_common_data_structure(result)
        mock_make_resultrow_common.assert_not_called()

    @pytest.mark.parametrize("handler", ["fetchone", "fetchall"])
    def test_common_data_structure_is_serializable(self, handler, mock_cursor, mock_hook):
        hook = mock_hook
        result = getattr(mock_cursor, handler)()
        common_result = hook._make_common_data_structure(result)
        json.dumps(common_result)


class TestSapHanaHookStreamRecords:
    def test_stream_rows_fetchone_not_called_until_next_called_on_generator(
        self, mock_conn, mock_cursor, mock_hook
    ):
        hook = mock_hook
        hook.get_conn = mock.Mock(return_value=mock_conn)
        mock_conn.cursor.return_value = mock_cursor

        results = hook._stream_records(mock_conn, mock_cursor)
        mock_cursor.fetchone.assert_not_called()
        next(results)
        mock_cursor.fetchone.assert_called_once()
        list(results)
        assert mock_cursor.fetchone.call_count == 4

    def test_stream_rows_resources_closed_when_cursor_exhausted(self, mock_conn, mock_cursor, mock_hook):
        hook = mock_hook
        hook.get_conn = mock.Mock(return_value=mock_conn)
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.connection = mock_conn

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
    def test_stream_rows_resources_closed_on_exception(
        self, exception, message, mock_conn, mock_cursor, mock_hook
    ):
        hook = mock_hook
        hook.get_conn = mock.Mock(return_value=mock_conn)
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.connection = mock_conn

        mock_cursor.fetchone.side_effect = exception(message)

        results = hook._stream_records("SELECT mock FROM dummy", mock_cursor)
        with pytest.raises(exception):
            next(results)
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_cursor_description_is_available_immediately(self, mock_conn, mock_cursor, mock_hook):
        hook = mock_hook
        hook.get_conn = mock.Mock(return_value=mock_conn)
        mock_conn.cursor.return_value = mock_cursor

        hook._stream_records(hook, mock_cursor)
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

    def test_make_cursor_description_available_immediately_resources_closed_on_exception(
        self, mock_conn, mock_cursor, mock_hook
    ):
        hook = mock_hook
        hook.get_conn = mock.Mock(return_value=mock_conn)
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = ProgrammingError("Bad SQL statement")

        with pytest.raises(ProgrammingError):
            hook.stream_records("SELECT mock FROM dummy")

        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()


class TestSapHanaHookBulkInsertRows:
    @pytest.mark.parametrize("is_generator", [True, False])
    def test_get_sample_row_returns_sample_row_and_copy_original_rows(
        self, is_generator, mock_hook, mock_insert_values
    ):
        hook = mock_hook
        rows = mock_insert_values(generator=is_generator)

        sample_row, new_rows = hook._get_sample_row(rows)
        assert sample_row == (
            "mock1",
            "mock2",
        )
        if is_generator:
            assert isinstance(new_rows, Iterator)
            assert len(list(new_rows)) == 20
        else:
            assert isinstance(new_rows, list)
            assert len(new_rows) == 20

    @pytest.mark.parametrize("is_generator", [True, False])
    @mock.patch("airflow_provider_sap_hana.hooks.hana.tee")
    def test_get_sample_row_tee_called(self, mock_tee, is_generator, mock_hook, mock_insert_values):
        hook = mock_hook
        rows = mock_insert_values(generator=is_generator)
        mock_tee.return_value = rows, rows

        hook._get_sample_row(rows)
        if not is_generator:
            mock_tee.assert_not_called()
        else:
            mock_tee.assert_called_once_with(rows, 2)

    @pytest.mark.parametrize("is_generator", [True, False])
    def test_prepare_cursor(
        self,
        is_generator,
        mock_conn,
        mock_dml_cursor,
        mock_hook,
        mock_insert_values,
    ):
        hook = mock_hook
        hook.get_conn = mock.Mock(return_value=mock_conn)
        mock_conn.cursor.return_value = mock_dml_cursor

        rows = mock_insert_values(generator=is_generator)
        sample_row, new_rows = hook._get_sample_row(rows)

        expected_sql = hook._generate_insert_sql("mock", sample_row, ["mock_col1", "mock_col2"])
        hook.bulk_insert_rows(table="mock", rows=new_rows, target_fields=["mock_col1", "mock_col2"])
        mock_dml_cursor.prepare.assert_called_once_with(expected_sql, newcursor=False)

    @pytest.mark.parametrize(
        "is_generator, commit_every, expected_call_count",
        [
            (True, 0, 1),
            (True, 5, 4),
            (True, 10, 2),
            (True, 15, 2),
            (False, 0, 1),
            (False, 5, 4),
            (False, 10, 2),
            (False, 15, 2),
        ],
    )
    def test_bulk_insert_rows_batches(
        self,
        is_generator,
        commit_every,
        expected_call_count,
        mock_conn,
        mock_dml_cursor,
        mock_insert_values,
        mock_hook,
    ):
        hook = mock_hook
        hook.get_conn = mock.Mock(return_value=mock_conn)
        mock_conn.cursor.return_value = mock_dml_cursor

        rows = mock_insert_values(generator=is_generator)
        hook.bulk_insert_rows(table="mock", rows=rows, commit_every=commit_every)

        assert mock_dml_cursor.executemanyprepared.call_count == expected_call_count

    @pytest.mark.parametrize(
        "autocommit, commit_every, expected_call_count",
        [(True, 0, 0), (False, 0, 1), (True, 5, 0), (False, 5, 4)],
    )
    def test_bulk_insert_rows_autocommit(
        self,
        autocommit,
        commit_every,
        expected_call_count,
        mock_conn,
        mock_dml_cursor,
        mock_insert_values,
        mock_hook,
    ):
        hook = mock_hook
        hook.get_conn = mock.Mock(return_value=mock_conn)
        mock_conn.cursor.return_value = mock_dml_cursor

        rows = mock_insert_values()

        hook.bulk_insert_rows(table="mock", rows=rows, commit_every=commit_every, autocommit=autocommit)
        assert mock_conn.commit.call_count == expected_call_count

    # TODO Remove after logging issues resolved in airflow 3.1.0
    @pytest.mark.xfail(
        get_base_airflow_version_tuple() == (3, 1, 0), reason="Logging not working in airflow 3.1"
    )
    @pytest.mark.parametrize(
        "commit_every, expected_rowcount", [(0, None), (5, [5, 10, 15, 20]), (10, [10, 20]), (15, [15, 20])]
    )
    def test_bulk_insert_rows_rowcount_logging(
        self,
        commit_every,
        expected_rowcount,
        mock_conn,
        mock_dml_cursor,
        mock_insert_values,
        mock_hook,
        caplog,
    ):
        hook = mock_hook
        hook.get_conn = mock.Mock(return_value=mock_conn)
        mock_conn.cursor.return_value = mock_dml_cursor

        rows = mock_insert_values()
        with caplog.at_level(20):
            hook.bulk_insert_rows(
                table="mock",
                rows=rows,
                commit_every=commit_every,
                target_fields=["mock_col1", "mock_col2"],
            )
        assert "Prepared statement: INSERT INTO mock (mock_col1, mock_col2) VALUES (?,?)" in caplog.messages
        if expected_rowcount:
            for call in expected_rowcount:
                assert f"Loaded {call} rows into mock so far" in caplog.messages
        assert "Done loading. Loaded a total of 20 rows into mock" in caplog.messages
