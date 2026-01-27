import asyncio
from mcp.client import sse
from mcp.client.session import ClientSession
import traceback

async def test_connection():
    MCP_URL = "http://127.0.0.1:8000"
    
    try:
        print(f"Connecting to {MCP_URL}...")
        async with sse.sse_client(MCP_URL) as (read, write):
            print("✓ Connection established")
            
            session = ClientSession(read, write)
            print("✓ Session created")
            
            print("Initializing...")
            init = await asyncio.wait_for(session.initialize(), timeout=10.0)
            print(f"✓ Initialized! Protocol version: {init.protocolVersion}")
            print(f"✓ Server info: {init.serverInfo}")
            
            # List available tools
            tools_result = await session.list_tools()
            print(f"\n✓ Available tools ({len(tools_result.tools)}):")
            for tool in tools_result.tools:
                print(f"  - {tool.name}")
            
            print("\n✅ MCP server is working correctly!")
            
    except asyncio.TimeoutError:
        print("❌ Timeout - server not responding")
        traceback.print_exc()
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_connection())