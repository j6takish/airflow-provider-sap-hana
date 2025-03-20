from __future__ import annotations

from functools import wraps


def make_cursor_description_available_immediately(func):
    """
    Ensure that the cursor description is available immediately after executing the SQL statement.

    The hook attributes 'descriptions' and 'last_description' will be available without having to first call
    'next' on the generator returned by the 'stream_rows' method.

    :param func: The function to decorate, typically one that streams rows from a cursor.
    :return: The function which yields rows from a cursor.
    """

    @wraps(func)
    def wrapper(self, sql, parameters):
        conn = None
        cur = None
        try:
            conn = self.get_conn()
            cur = conn.cursor()
            self._run_command(cur, sql, parameters)
            self.descriptions.append(cur.description)
        except Exception as e:
            if cur:
                cur.close()
            if conn:
                conn.close()
            raise e

        return func(self, cur)

    return wrapper
