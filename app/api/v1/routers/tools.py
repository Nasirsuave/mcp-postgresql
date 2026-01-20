# from fastapi import FastAPI
# from app.schemas.tool import ToolRequest
# from mcp.client import streamable_http
# from mcp.client.session import ClientSession


# app = FastAPI()


# @app.post("/tool")
# async def call_tool(request: ToolRequest):
#     # 1️⃣ Decide which MCP tool to call
#     tool_name = request.tool
#     tool_input = request.input

#     mcp_url = "http://localhost:8000/mcp"

#     async with streamable_http.streamablehttp_client(mcp_url) as (read, write, _):
#             session = ClientSession(read, write)

#             await session.initialize()

#             # 2. Ask MCP server what tools exist
#             tools_response = await session.list_tools()
#             allowed_tools = tools_response.tools
#     # 2️⃣ Validate tool_name
#     if tool_name not in allowed_tools:
#         return {"error": "Tool not allowed"}

#     # 3️⃣ Call the MCP tool
#     if tool_name == "run_query":
#         result = run_query(tool_input)
#     elif tool_name == "list_tables":
#         result = list_tables(tool_input)
#     elif tool_name == "list_schemas":
#         result = list_schemas(tool_input)
#     # … etc

#     # 4️⃣ Return result to user
#     return {"result": result}






from fastapi import FastAPI, HTTPException, APIRouter
from app.schemas.tool import ToolRequest
from mcp.client import streamable_http
from mcp.client.session import ClientSession
import asyncio



router = APIRouter()



# MCP_URL = "http://localhost:8000/mcp"
MCP_URL = "http://localhost:8000/mcp"



ALLOWED_TOOLS = {
    "run_query",
    "list_tables",
    "list_schemas",
}

@router.post("/tool")
async def call_tool(request: ToolRequest):
    tool_name = request.tool
    tool_input = request.input

    # 1️⃣ Validate tool
    if tool_name not in ALLOWED_TOOLS:
        raise HTTPException(status_code=400, detail="Tool not allowed")

    # 2️⃣ Connect to MCP
    try:
        async with streamable_http.streamablehttp_client(MCP_URL) as (read, write, _):
            session = ClientSession(read, write)
            print("session value:", session)
            # init = await asyncio.wait_for(session.initialize(), timeout=5.0)
            # print("protocol:", init.protocolVersion)
            await session.initialize()


            # 3️⃣ Call MCP tool
            result = await asyncio.wait_for(
                session.call_tool(tool_name,
                    {"input": tool_input}  # ✅ REQUIRED WRAP
                ),
                timeout=10.0
            )
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="MCP server timeout - ensure server is running at " + MCP_URL)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"MCP server error: {str(e)}")

    # 4️⃣ Return MCP result
    return result
