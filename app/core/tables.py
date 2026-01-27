from typing import Any, Dict, List, Optional
from app.core.exec import exec_query
from app.core.meta import get_current_schema  # or wherever this lives
from app.core.exec import query, _get_current_schema
# from app.core.tables import describe_table
import logging

def list_tables_json(
    *,
    db_schema: Optional[str],
    name_like: Optional[str],
    case_sensitive: bool,
    table_types: Optional[List[str]],
    row_limit: int,
) -> List[Dict[str, Any]]:

    eff_schema = db_schema or get_current_schema()

    conditions = ["table_schema = %s"]
    params: List[Any] = [eff_schema]

    if name_like:
        pattern = name_like.replace("*", "%").replace("?", "_")
        op = "LIKE" if case_sensitive else "ILIKE"
        conditions.append(f"table_name {op} %s")
        params.append(pattern)

    if table_types:
        placeholders = ",".join(["%s"] * len(table_types))
        conditions.append(f"table_type IN ({placeholders})")
        params.extend(table_types)

    where_clause = " AND ".join(conditions)

    sql = f"""
    SELECT table_name, table_type
    FROM information_schema.tables
    WHERE {where_clause}
    ORDER BY table_name
    LIMIT %s
    """
    params.append(row_limit)

    res = exec_query(sql, params, row_limit, as_json=True)
    return res if isinstance(res, list) else []





logger = logging.getLogger("tables")
def describe_table(
    *,
    table_name: str,
    db_schema: Optional[str] = None,
) -> str:
    eff_schema = db_schema or _get_current_schema()
    logger.info(f"Describing table: {eff_schema}.{table_name}")

    sql = """
    SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default,
        character_maximum_length
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """

    return query(sql, [eff_schema, table_name])