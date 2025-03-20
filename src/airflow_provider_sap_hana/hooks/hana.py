from __future__ import annotations

from collections import deque
from collections.abc import Generator, Iterable, Mapping, Sequence
from contextlib import closing, suppress
from datetime import datetime
from textwrap import indent
from typing import TYPE_CHECKING, Any, TypeVar

import hdbcli.dbapi
from airflow.providers.common.sql.hooks.handlers import fetch_one_handler
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.utils.module_loading import import_string
from methodtools import lru_cache
from more_itertools import chunked
from sqlalchemy import inspect
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import NoSuchModuleError

from airflow_provider_sap_hana.hooks.decorators import make_cursor_description_available_immediately

if TYPE_CHECKING:
    from hdbcli.dbapi import Connection as HDBCLIConnection
    from hdbcli.resultrow import ResultRow
    from sqlalchemy_hana.dialect import HANAInspector

T = TypeVar("T")


class SapHanaHook(DbApiHook):
    """
    Interact with SAP HANA.

    Additional connection properties and SQLDBC properties can be passed as key: value pairs into the extra
    connection argument.

    :param enable_db_log_messages: If enabled, logs messages sent to the client during the session. The default options
    are 'SQL=INFO,FLUSH=ON'. If you wish to change the log level or any other options, pass in the 'traceOptions'
    keyword argument into the extra connection argument.
    """

    conn_name_attr = "hana_conn_id"
    default_conn_name = "hana_default"
    conn_type = "hana"
    hook_name = "SAP HANA"
    supports_autocommit = True
    supports_executemany = True
    _test_connection_sql = "SELECT 1 FROM dummy"
    _placeholder = "?"
    SQLALCHEMY_SCHEME = "hana+hdbcli"

    def __init__(self, *args, enable_db_log_messages: bool = False, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)
        self.enable_db_log_messages = enable_db_log_messages
        self.db_log_messages: deque = deque(maxlen=50)

    @property
    def replace_statement_format(self) -> str:
        """
        SAP HANA uses 'UPSERT' as it's replace statement.

        Using the 'WITH PRIMARY KEY' clause is recommended syntax for SAP HANA. This is orders of magnitude faster
        than using 'UPSERT' without the additional clause.
        """
        if self._replace_statement_format is None:
            self._replace_statement_format = self.connection_extra.get(
                "replace_statement_format", "UPSERT {} {} VALUES ({}) WITH PRIMARY KEY"
            )
        return self._replace_statement_format

    @property
    def replace_statement_format_backup(self) -> str:
        """This is a backup replace statement for working with tables that do not have a primary key."""
        return "UPSERT {} {} VALUES ({})"

    @lru_cache(maxsize=None)
    def get_reserved_words(self, dialect_name: str) -> set[str]:
        result = set()
        with suppress(ImportError, ModuleNotFoundError, NoSuchModuleError):
            dialect_module = import_string(self.sqlalchemy_url.get_dialect().__module__)
            if hasattr(dialect_module, "RESERVED_WORDS"):
                result = set(dialect_module.RESERVED_WORDS)
        self.log.debug("reserved words for '%s': %s", dialect_name, result)
        return result

    @property
    def sqlalchemy_url(self) -> URL:
        connection = self.connection
        return URL.create(
            drivername=self.SQLALCHEMY_SCHEME,
            host=connection.host,
            username=connection.login,
            password=connection.password,
            port=connection.port,
            database=self.schema or connection.schema,
        )

    @property
    def inspector(self) -> HANAInspector:
        """
        Override the DbApiHook 'inspector' property.

        The Inspector used for SAP HANA is an
        instance of HANAInspector and offers an additional method,
        which returns the OID (object id) for the given table name.

        :return: A HANAInspector object.
        """
        engine = self.get_sqlalchemy_engine()
        return inspect(engine)

    def get_conn(self) -> HDBCLIConnection:
        """
        Connect to a SAP HANA database.

        The address, user, password, and port are extracted from the Airflow Connection.
        Additional connection properties and SQLDBC properties can be passed as key: value pairs into the extra
        connection argument.

        :return: A hdbcli Connection object.
        """
        connection = self.connection
        conn_args = {
            "address": connection.host,
            "user": connection.login,
            "password": connection.password,
            "port": connection.port,
            "databasename": self.schema or connection.schema,
        }
        for key, val in self.connection_extra_lower.items():
            conn_args[key] = val
        trace_options = conn_args.pop("traceoptions", "SQL=INFO,FLUSH=ON")
        conn = hdbcli.dbapi.connect(**conn_args)
        if self.enable_db_log_messages:
            conn.ontrace(self._log_message, trace_options)  # noqa: hdbcli says ontrace takes no arguments but it does
        return conn

    def _log_message(self, message: str) -> None:
        lines = message.splitlines(True)
        if lines and "libSQLDBCHDB" in lines[0]:
            lines[0] = "\n" + lines[0]
        joined = "".join(lines)
        indented = indent(joined, prefix="    ")
        self.db_log_messages.append(indented)

    def set_autocommit(self, conn: HDBCLIConnection, autocommit: bool) -> None:
        """
        Override the DbApiHook 'set_autocommit' method.

        hdbcli uses an autocommit method and not an autocommit attribute.

        :param conn: A hdbcli Connection object to set autocommit.
        :param autocommit: bool.
        :return: None.
        """
        if self.supports_autocommit:
            conn.setautocommit(autocommit)

    def get_autocommit(self, conn: HDBCLIConnection) -> bool | None:
        """
        Override the DbApiHook 'set_autocommit' method.

        hdbcli uses an autocommit method and not an autocommit attribute.

        :param conn: A hdbcli Connection object to get autocommit setting from.
        :return: bool.
        """
        if self.supports_autocommit:
            return conn.getautocommit()
        return None

    @staticmethod
    def _make_resultrow_cell_serializable(cell: Any) -> Any:
        """
        Convert a ResultRow datetime value to string.

        This is a custom method to make SAP HANA result sets JSON serializable. This method differs from the
        DbApiHook method 'serialize_cells' in that this method is intended to work with data exiting SAP HANA via
        SELECT statements. Datimetime values are converted to str using the datetime 'isoformat' method. All other
        data types (str, int, float, None) are unchanged.

        The DbApiHook method 'serialize_cells' is still called when data is entering SAP HANA via DML statements.

        :param cell: The input cell, which can be of any type.
        :return: The input `cell`, converted to a string if it is a `datetime`, or unchanged if it is of any other type
        """
        if isinstance(cell, datetime):
            return cell.isoformat()
        return cell

    @classmethod
    def _make_resultrow_common(cls, row: ResultRow) -> tuple:
        """
        Convert a ResultRow into a common tuple.

        This is a custom method to make SAP HANA result sets JSON serializable.
        ResultRow objects are not JSON serializable so they must be converted into a tuple.

        :param row: A ResultRow object.
        :return: A tuple with all 'datetime' values converted to string, or unchanged if they are of any other type
        """
        return tuple(map(cls._make_resultrow_cell_serializable, row))

    def _make_common_data_structure(self, result: T | Sequence[T]) -> tuple | list[tuple] | None:
        """
        Override the DbApiHook '_make_common_data_structure' method.

        This is a custom method to make SAP HANA result sets JSON serializable.
        ResultRow objects are not JSON serializable so they must be converted into a tuple or a list of tuples.

        :param result: A list of ResultRow objects if the 'fetchall' handler is used,
        a single ResultRow if the 'fetchone' handler is used.
        :return: A list of tuples if the 'fetchall' handler is used. A single tuple if the 'fetchone' handler is used.
        """
        if not result:
            return result
        if isinstance(result, Sequence):
            return list(map(self._make_resultrow_common, result))
        return self._make_resultrow_common(result)

    def get_primary_keys(self, table: str, schema: str | None = None) -> list[str] | None:
        """
        Get the primary key or primary keys for a given table.

        :param table: The table name.
        :param schema: The schema where the table is located.
        :return: A list of primary keys or None if the table cannot be found or has no primary keys.
        """
        return self.dialect.get_primary_keys(table, schema)

    @make_cursor_description_available_immediately
    def _stream_records(self, cur):
        try:
            row = self._make_common_data_structure(fetch_one_handler(cur))
            while row:
                yield row
                row = self._make_common_data_structure(fetch_one_handler(cur))
        finally:
            cur.close()
            cur.connection.close()

    def stream_records(
        self, sql: str, parameters: Iterable | Mapping[str, Any] | None = None
    ) -> Generator[tuple[Any]]:
        """
        Streams records from SAP HANA, yielding one row at a time.

        This is a custom method to fetch large amounts of records without loading them all into memory at once.
        Each record is passed through the '_make_common_data_structure' method to ensure it is JSON serializable.
        The hook attributes 'descriptions' and 'last_description' are available immediately after executing the
        SQL statement, without having to first call 'next' on the iterator.

        :param: sql: The sql statement.
        :param: parameters: The parameters to be bound to the sql statement.
        :return: An iterator of tuples.
        """
        self.descriptions = []
        return self._stream_records(sql, parameters)

    def bulk_insert_rows(
        self,
        table: str,
        rows: list,
        target_fields: list | None = None,
        commit_every: int = 10000,
        replace: bool = False,
        autocommit: bool = True,
    ) -> None:
        """
        Insert records into SAP HANA using a prepared statement.

        This is a custom method to insert records as efficiently as possible.
        hdbcli Cursors do not have a 'fast_executemany' attribute, but it can be replicated using prepared statements.
        Prepared statements also have significantly less overhead due fewer calls to the database.

        :param table: The table name.
        :param rows: The rows to insert into the table.
        :param target_fields: The names of the columns to fill in the table.
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :param replace: Whether to replace instead of insert.
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :return: None.
        """
        nb_rows = 0
        sql = self._generate_insert_sql(table, rows[0], target_fields, replace)
        chunksize = len(rows) if not commit_every else commit_every
        chunked_serialized_rows = chunked(map(self._serialize_cells, rows), chunksize)
        with self._create_autocommit_connection(autocommit) as conn:
            with closing(conn.cursor()) as cur:
                cur.prepare(sql, newcursor=False)
                if self.log_sql:
                    self.log.info("Prepared statement: %s", sql)

                for chunk in chunked_serialized_rows:
                    cur.executemanyprepared(chunk)
                    if not autocommit:
                        conn.commit()
                    nb_rows += cur.rowcount
                    self.log.info("Loaded %s rows into %s so far", nb_rows, table)
        self.log.info("Done loading. Loaded a total of %s rows into %s", nb_rows, table)

    def get_db_log_messages(self, conn: None = None) -> None:
        if self.db_log_messages:
            self.log.info("".join(self.db_log_messages))
