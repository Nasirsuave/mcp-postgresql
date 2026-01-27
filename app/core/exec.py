import time
from typing import Any, List, Optional
from psycopg.rows import dict_row
from .db import get_connection, READONLY
import json
from .db import CONNECTION_STRING

def is_select_like(sql: str) -> bool:
    token = sql.lstrip().split(" ", 1)[0].lower()
    return token in {"select", "with", "show", "values", "explain"}

def exec_query(
    sql: str,
    parameters: Optional[List[Any]],
    row_limit: int,
    as_json: bool,
):
    conn = get_connection()

    if READONLY and not is_select_like(sql):
        return [] if as_json else "Read-only mode enabled"

    with conn.cursor(row_factory=dict_row) as cur:
        if parameters:
            cur.execute(sql, parameters)
        else:
            cur.execute(sql)

        if cur.description is None:
            conn.commit()
            return [] if as_json else f"Rows affected: {cur.rowcount}"

        rows = cur.fetchmany(row_limit)
        if as_json:
            return [dict(r) for r in rows]

        if not rows:
            return "No results found"

        keys = rows[0].keys()
        out = [" | ".join(keys)]
        for r in rows:
            out.append(" | ".join(str(r[k]) for k in keys))
        return "\n".join(out)
    



#just added

def _get_current_schema() -> str:
    try:
        res = exec_query(     #changed here 
            "SELECT current_schema() AS schema",
            None,
            1,
            as_json=True
        )
        if isinstance(res, list) and res:
            schema = res[0].get("schema")
            if isinstance(schema, str) and schema:
                return schema
    except Exception:
        pass

    return "public"




def query(
    sql: str,
    parameters: Optional[List[Any]] = None,
    row_limit: int = 500,
    format: str = "markdown",
) -> str:
    """Execute a SQL query (legacy signature)."""
    if not CONNECTION_STRING:
        return "POSTGRES_CONNECTION_STRING is not set. Provide --conn DSN or export POSTGRES_CONNECTION_STRING."

    as_json = (format.lower() == "json")
    res = exec_query(sql, parameters, row_limit, as_json) #changed here 

    if as_json and not isinstance(res, str):
        try:
            return json.dumps(res, default=str)
        except Exception as e:
            return f"JSON encoding error: {e}"

    return res