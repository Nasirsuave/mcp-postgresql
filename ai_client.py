print("ai_client.py loaded")

import asyncio
import os
from openai import OpenAI
from dotenv import load_dotenv 
from mcp.client import streamable_http
from mcp.client.streamable_http import streamablehttp_client #just added
from mcp.client.stdio import stdio_client   #just added
from mcp.client.session import ClientSession


load_dotenv()  # Load environment variables from .env file



async def main():
    # 1. Connect to MCP server
    print("Connecting to MCP server...")

    try:
        mcp_url = "http://localhost:8000/mcp"

        # async with streamable_http.streamablehttp_client(mcp_url) as (read, write, _):
        # async with streamablehttp_client("http://localhost:8000/mcp") as (read, write, _):
        async with stdio_client() as session:
            # session = ClientSession(read, write)

            await session.initialize()
            print("Connected to MCP server.")
            # 2. Ask MCP server what tools exist
            tools_response = await session.list_tools()
            mcp_tools = tools_response.tools
    except Exception as e:
             print("Error inside MCP session block:", e)

    finally:
        print(f" MCP connected with {len(mcp_tools)} tools available.")
        # Convert MCP tools into OpenAI tool format
        OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

        openai_tools = []
        for tool in mcp_tools:
            openai_tools.append(
                {
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": tool.description or "",
                        "parameters": tool.inputSchema or {"type": "object"},
                    },
                }
            )

        # 3. Ask the AI a natural language question
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

        # user_question = "What tables are in my database?"
        user_question = "Show me all users."

        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "You are a database assistant. Use tools when needed.",
                },
                {
                    "role": "user",
                    "content": user_question,
                },
            ],
            tools=openai_tools,
            tool_choice="auto",
        )

        message = response.choices[0].message
        print("\nRAW AI MESSAGE:")
        print(message)

        # 4. If the AI decided to call a tool
        if message.tool_calls:
            tool_call = message.tool_calls[0]
            tool_name = tool_call.function.name 
            tool_args = tool_call.function.arguments

            print(f"\nAI decided to call tool: {tool_name}")
            print(f"With arguments: {tool_args}")

            # Call the MCP tool
            result = await session.call_tool(
                tool_name,
                {"input": tool_args},
            )

            print("\nTool result:")
            print(result.structuredContent or result.content)

        else:
            print("\nAI response (no tool call):")
            print(message.content)


if __name__ == "__main__":
    print("ABOUT TO CALL asyncio.run")
    asyncio.run(main())
    print("AFTER asyncio.run")
