import psycopg
from psycopg.rows import dict_row
import os
import logging
from typing import Optional, List, Any
import json
# from .exec import _exec_query



logger = logging.getLogger("db")

CONNECTION_STRING: Optional[str] = os.getenv("POSTGRES_CONNECTION_STRING")

READONLY: bool = os.getenv("POSTGRES_READONLY", "false").lower() in {"1", "true", "yes"}

STATEMENT_TIMEOUT_MS: Optional[int] = None
if os.getenv("POSTGRES_STATEMENT_TIMEOUT_MS"):
    STATEMENT_TIMEOUT_MS = int(os.getenv("POSTGRES_STATEMENT_TIMEOUT_MS"))

def get_connection():
    if not CONNECTION_STRING:
        raise RuntimeError("POSTGRES_CONNECTION_STRING not set")

    conn = psycopg.connect(CONNECTION_STRING)
    with conn.cursor() as cur:
        # cur.execute("SET application_name = %s", ("mcp-postgres",))
        cur.execute("SET application_name = 'mcp-postgres'")
        if STATEMENT_TIMEOUT_MS:
            # cur.execute("SET statement_timeout = %s", (STATEMENT_TIMEOUT_MS,))
            cur.execute(f"SET statement_timeout = {STATEMENT_TIMEOUT_MS}")

        conn.commit()
    return conn




# def _get_current_schema() -> str:
#     try:
#         res = _exec_query(     #changed here 
#             "SELECT current_schema() AS schema",
#             None,
#             1,
#             as_json=True
#         )
#         if isinstance(res, list) and res:
#             schema = res[0].get("schema")
#             if isinstance(schema, str) and schema:
#                 return schema
#     except Exception:
#         pass

#     return "public"




# def query(
#     sql: str,
#     parameters: Optional[List[Any]] = None,
#     row_limit: int = 500,
#     format: str = "markdown",
# ) -> str:
#     """Execute a SQL query (legacy signature)."""
#     if not CONNECTION_STRING:
#         return "POSTGRES_CONNECTION_STRING is not set. Provide --conn DSN or export POSTGRES_CONNECTION_STRING."

#     as_json = (format.lower() == "json")
#     res = _exec_query(sql, parameters, row_limit, as_json) #changed here 

#     if as_json and not isinstance(res, str):
#         try:
#             return json.dumps(res, default=str)
#         except Exception as e:
#             return f"JSON encoding error: {e}"

#     return res




