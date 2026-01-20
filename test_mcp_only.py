import asyncio
from mcp.client import streamable_http
from mcp.client.session import ClientSession

async def main():
   url = "http://localhost:8000/mcp"
   async with streamable_http.streamablehttp_client(url) as (read, write, _):
        session = ClientSession(read, write)

        print("Connected!")

asyncio.run(main())
