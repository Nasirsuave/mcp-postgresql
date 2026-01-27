from pydantic import BaseModel, Field
from typing import Optional, List, Any, Literal, Dict
from app.core.exec import exec_query
import base64
import json

class QueryInput(BaseModel):
    sql: str
    parameters: Optional[List[Any]] = None
    row_limit: int = Field(default=500, ge=1, le=10000)
    format: Literal["markdown", "json"] = "markdown"

class QueryJSONInput(BaseModel):
    sql: str
    parameters: Optional[List[Any]] = None
    row_limit: int = 500

class ListSchemasInput(BaseModel):
    include_system: bool = False
    include_temp: bool = False
    require_usage: bool = True
    row_limit: int = 10000



class ListSchemasRequest(BaseModel):
    include_system: bool = False
    include_temp: bool = False
    require_usage: bool = True
    name_like: Optional[str] = None
    case_sensitive: bool = False
    row_limit: int = 100


class ListSchemasPageRequest(BaseModel):
    include_system: bool = False
    include_temp: bool = False
    require_usage: bool = False
    name_like: Optional[str] = None
    case_sensitive: bool = False
    page_size: int = 50
    cursor: Optional[str] = None



class ListTablesInput(BaseModel):
    db_schema: Optional[str] = None
    name_like: Optional[str] = None
    case_sensitive: bool = False
    table_types: Optional[List[str]] = None
    row_limit: int = Field(default=500, ge=1, le=10000)






class DescribeTableRequest(BaseModel):
    table_name: str
    db_schema: Optional[str] = None






def list_schemas(
    *,
    include_system: bool,
    include_temp: bool,
    require_usage: bool,
    name_like: str | None,
    case_sensitive: bool,
    row_limit: int,
) -> List[Dict[str, Any]]:

    conditions = []
    params: List[Any] = []

    if not include_system:
        conditions.append("NOT (n.nspname = 'information_schema' OR n.nspname LIKE 'pg_%%')") #changed here
    if not include_temp:
        conditions.append("n.nspname NOT LIKE 'pg_temp_%%'") #changed here
    if require_usage:
        conditions.append("has_schema.priv")

    if name_like:
        pattern = name_like.replace('*', '%').replace('?', '_')
        op = 'LIKE' if case_sensitive else 'ILIKE'
        conditions.append(f"n.nspname {op} %s")
        params.append(pattern)

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    limit_clause = " LIMIT %s"
    params.append(row_limit)

    sql = f"""
    WITH has_schema AS (
        SELECT n.oid AS oid, has_schema_privilege(n.nspname, 'USAGE') AS priv
        FROM pg_namespace n
    )
    SELECT 
        n.nspname AS schema_name,
        pg_get_userbyid(n.nspowner) AS owner,
        (n.nspname = 'information_schema' OR n.nspname LIKE 'pg_%%') AS is_system,
        (n.nspname LIKE 'pg_temp_%%') AS is_temporary,
        has_schema.priv AS has_usage
    FROM pg_namespace n
    JOIN has_schema ON has_schema.oid = n.oid
    {where_clause}
    ORDER BY n.nspname
    {limit_clause}
    """

    res = exec_query(sql, params, row_limit, as_json=True)
    return res if isinstance(res, list) else []




def list_schemas_json_page(
    *,
    include_system: bool,
    include_temp: bool,
    require_usage: bool,
    name_like: str | None,
    case_sensitive: bool,
    page_size: int,
    cursor: str | None,
) -> Dict[str, Any]:
    # Decode cursor
    offset = 0
    if cursor:
        try:
            payload = json.loads(base64.b64decode(cursor).decode("utf-8"))
            offset = int(payload.get("offset", 0))
        except Exception:
            offset = 0

    conditions = []
    params: List[Any] = []

    if not include_system:
        conditions.append(
            "NOT (n.nspname = 'information_schema' OR n.nspname LIKE 'pg_%%')"
        )
    if not include_temp:
        conditions.append("n.nspname NOT LIKE 'pg_temp_%%'")
    if require_usage:
        conditions.append("has_schema.priv")

    if name_like:
        pattern = name_like.replace("*", "%").replace("?", "_")
        op = "LIKE" if case_sensitive else "ILIKE"
        conditions.append(f"n.nspname {op} %s")
        params.append(pattern)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    limit = page_size + 1  # fetch one extra row

    sql = f"""
    WITH has_schema AS (
        SELECT n.oid AS oid, has_schema_privilege(n.nspname, 'USAGE') AS priv
        FROM pg_namespace n
    )
    SELECT 
        n.nspname AS schema_name,
        pg_get_userbyid(n.nspowner) AS owner,
        (n.nspname = 'information_schema' OR n.nspname LIKE 'pg_%%') AS is_system,
        (n.nspname LIKE 'pg_temp_%%') AS is_temporary,
        has_schema.priv AS has_usage
    FROM pg_namespace n
    JOIN has_schema ON has_schema.oid = n.oid
    {where_clause}
    ORDER BY n.nspname
    LIMIT %s OFFSET %s
    """

    rows = exec_query(sql, params + [limit, offset], limit, as_json=True)

    items: List[Dict[str, Any]] = []
    next_cursor: Optional[str] = None

    if isinstance(rows, list):
        if len(rows) > page_size:
            items = rows[:page_size]
            next_cursor = base64.b64encode(
                json.dumps({"offset": offset + page_size}).encode("utf-8")
            ).decode("utf-8")
        else:
            items = rows

    return {"items": items, "next_cursor": next_cursor}





