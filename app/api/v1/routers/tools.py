
from fastapi import APIRouter, HTTPException
from app.core.exec import exec_query
from app.core.schemas import (
    QueryInput,
    QueryJSONInput,
    ListSchemasRequest,
    list_schemas,
    list_schemas_json_page,
    ListSchemasPageRequest,
    ListTablesInput,
    DescribeTableRequest
)
from app.core.tables import list_tables_json
from typing import Any, Dict, List
from app.core.db import CONNECTION_STRING
from app.core.tables import describe_table


router = APIRouter(
    # prefix="/tools",
    # tags=["tools"]
)

@router.post("/run-query")
def run_query(input: QueryInput):
    try:
        result = exec_query(
            sql=input.sql,
            parameters=input.parameters,
            row_limit=input.row_limit,
            as_json=(input.format == "json"),
        )
        return {
            "success": True,
            "result": result
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))




@router.post("/run-query-json", response_model=List[Dict[str, Any]])
def run_query_json_api(req: QueryJSONInput):
    """
    REST wrapper around MCP run_query_json
    """
    if not CONNECTION_STRING:
        raise HTTPException(status_code=500, detail="POSTGRES_CONNECTION_STRING not set")

    result = exec_query(
        sql=req.sql,
        parameters=req.parameters,
        row_limit=req.row_limit,
        as_json=True,
    )

    if not isinstance(result, list):
        return []

    return result





@router.post("/list-schemas", response_model=List[Dict[str, Any]])
def list_schemas_api(req: ListSchemasRequest):
    if not CONNECTION_STRING:
        raise HTTPException(
            status_code=500,
            detail="POSTGRES_CONNECTION_STRING not set"
        )

    return list_schemas(
        include_system=req.include_system,
        include_temp=req.include_temp,
        require_usage=req.require_usage,
        name_like=req.name_like,
        case_sensitive=req.case_sensitive,
        row_limit=req.row_limit,
    )





@router.post("/schemas/page", response_model=Dict[str, Any])
def list_schemas_page_api(req: ListSchemasPageRequest):
    return list_schemas_json_page(
        include_system=req.include_system,
        include_temp=req.include_temp,
        require_usage=req.require_usage,
        name_like=req.name_like,
        case_sensitive=req.case_sensitive,
        page_size=req.page_size,
        cursor=req.cursor,
    )






@router.post("/list/tables", response_model=List[Dict[str, Any]])
def list_tables_api(req: ListTablesInput):
    return list_tables_json(
        db_schema=req.db_schema,
        name_like=req.name_like,
        case_sensitive=req.case_sensitive,
        table_types=req.table_types,
        row_limit=req.row_limit,
    )







@router.post("/describe/table")
def describe_table_api(req: DescribeTableRequest):
    result = describe_table(
        table_name=req.table_name,
        db_schema=req.db_schema,
    )

    if not result:
        raise HTTPException(
            status_code=404,
            detail="Table not found or has no columns"
        )

    return {
        "table": req.table_name,
        "schema": req.db_schema or "current",
        "description": result,
    }

