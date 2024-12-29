from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, TypeVar

import hdbcli.dbapi
from airflow.providers.common.sql.hooks.sql import DbApiHook
from sqlalchemy import inspect
from sqlalchemy.engine.url import URL

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

    :param args: Arguments passed to DbApiHook.
    :param kwargs: Keyword arguments passed to DbApiHook.
    """

    conn_name_attr = "hana_conn_id"
    default_conn_name = "hana_default"
    conn_type = "hana"
    hook_name = "SAP HANA"
    supports_autocommit = True
    supports_executemany = True
    _test_connection_sql = "SELECT 1 FROM dummy"
    _placeholder = "?"
    _sqlalchemy_driver = "hana+hdbcli"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)
        self._replace_statement_format = kwargs.get(
            "replace_statement_format", "UPSERT {} {} VALUES ({}) WITH PRIMARY KEY"
        )

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
        for key, val in connection.extra_dejson.items():
            conn_args[key] = val
        return hdbcli.dbapi.connect(**conn_args)

    @property
    def sqlalchemy_url(self) -> URL:
        connection = self.connection
        return URL.create(
            drivername=self._sqlalchemy_driver,
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

        The Inspector used for the SAP HANA database is an
        instance of HANAInspector and offers an additional method
        which returns the OID (object id) for the given table name.

        :return: A HANAInspector object.
        """
        engine = self.get_sqlalchemy_engine()
        return inspect(engine)

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

    def get_autocommit(self, conn: HDBCLIConnection) -> bool:
        """
        Override the DbApiHook 'set_autocommit' method.

        hdbcli uses an autocommit method and not an autocommit attribute.

        :param conn: A hdbcli Connection object to get autocommit setting from.
        :return: bool.
        """
        if self.supports_autocommit:
            return conn.getautocommit()

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
    def _make_resultrow_common(cls, row: ResultRow | None) -> tuple:
        """
        Convert a ResultRow into a common tuple.

        This is a custom method to make SAP HANA result sets JSON serializable.
        ResultRow objects are not JSON serializable so they must be converted into a tuple.

        :param row: A ResultRow object.
        :return: A tuple with all 'datetime' values converted to string, or unchanged if they are of any other type
        """
        return tuple(map(cls._make_resultrow_cell_serializable, row))

    def _make_common_data_structure(self, result: T | Sequence[T]) -> tuple | list[tuple]:
        """
        Override the DbApiHook '_make_common_data_structure' method.

        This is a custom method to make SAP HANA result sets JSON serializable.
        ResultRow objects are not JSON serializable so they must be converted into a tuple or a list of tuples.

        :param result: A list of ResultRow objects if the 'fetchall' handler is used,
        a single ResultRow if the 'fetchone' handler is used.
        :return: A list of tuples if the 'fetchall' handler is used. A single tuple if the 'fetchone' handler is used.
        """
        if isinstance(result, Sequence):
            return list(map(self._make_resultrow_common, result))
        return self._make_resultrow_common(result)

    def get_table_primary_key(self, table: str) -> list[str] | None:
        """
        Get the primary key or primary keys for a given table.

        This is a custom method to return primary keys for a table. Table must be passed in as fully qualified
        'SCHEMA_NAME.TABLE_NAME' as 'SCHEMA_NAME' is used in the WHERE clause of the query used to retrieve the
        primary keys.

        :param table: A fully qualified, 'SCHEMA_NAME.TABLE_NAME' table.
        :return: A list of primary keys or None if the table cannot be found or has no primary keys.
        """
        schema, table = table.split(".")
        sql = """
        SELECT column_name
        FROM SYS.CONSTRAINTS
        WHERE
            is_primary_key = 'TRUE'
            AND schema_name = ?
            AND table_name = ?
        """
        result = self.get_records(sql=sql, parameters=(schema, table))
        return [row[0] for row in result] if result else None
