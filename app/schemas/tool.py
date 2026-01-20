from pydantic import BaseModel
from typing import Any, Dict

class ToolRequest(BaseModel):
    tool: str
    input: Dict[str, Any]
