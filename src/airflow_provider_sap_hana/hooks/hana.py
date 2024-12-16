from __future__ import annotations

from airflow.providers.common.sql.hooks.sql import DbApiHook
from sqlalchemy.engine.url import URL
from sqlalchemy import inspect
import hdbcli.dbapi
from datetime import datetime
from typing import TYPE_CHECKING, TypeVar, Sequence, Any
if TYPE_CHECKING:
    from hdbcli.dbapi import Connection as HDBCLIConnection
    from hdbcli.resultrow import ResultRow
    from sqlalchemy_hana.dialect import HANAInspector

T = TypeVar("T")


class SapHanaHook(DbApiHook):
    conn_name_attr = "hana_conn_id"
    default_conn_name = "hana_default"
    conn_type = "hana"
    hook_name = "SAP HANA Hook"
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
        connection = self.connection
        conn_args = {
            "address": connection.host,
            "user": connection.login,
            "password":connection.password,
            "port": connection.port,
            "database": self.schema or connection.schema
        }
        return hdbcli.dbapi.connect(**conn_args)

    @property
    def sqlalchemy_url(self):
        connection = self.connection
        return URL.create(
            drivername=self._sqlalchemy_driver,
            host=connection.host,
            username=connection.login,
            password=connection.password,
            port=connection.port,
            database=self.schema or connection.schema
        )

    @property
    def inspector(self) -> HANAInspector:
        """
        Override the DbApiHook 'inspector' property.

        The Inspector used for the SAP HANA database is an
        instance of HANAInspector and offers an additional method
        which returns the OID (object id) for the given table name.
        """
        engine = self.get_sqlalchemy_engine()
        return inspect(engine)

    def set_autocommit(
        self,
        conn: HDBCLIConnection,
        autocommit: bool) -> None:
        if self.supports_autocommit:
            conn.setautocommit(autocommit)

    def get_autocommit(
        self,
        conn: HDBCLIConnection) -> bool:
        return conn.getautocommit()

    @staticmethod
    def _make_resultrow_cell_serializable(cell: Any) -> Any:
        if isinstance(cell, datetime):
            return cell.isoformat()
        return cell

    @classmethod
    def _make_resultrow_common(
            cls,
            row: ResultRow) -> tuple:
        return tuple(map(cls._make_resultrow_cell_serializable, row.column_values))

    def _make_common_data_structure(self, result: T | Sequence[T]) -> tuple | list[tuple]:
        """
        Overrides the DbApiHook '_make_common_data_structure' method.

        HDBCLI row results are of the custom class 'ResultRow'. 'ResultRow' has attributes
        for row.column_names and row.column_values.

        'RowResults' are not JSON serializable so they must be converted into a tuple or a list of tuples.
        """
        if isinstance(result, Sequence):
            return list(map(self._make_resultrow_common, result))
        return self._make_resultrow_common(result)
