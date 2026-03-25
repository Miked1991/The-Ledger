# src/mcp/server.py
import asyncio
import json
from typing import Any, Dict, Optional
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import asyncpg
from pydantic import BaseModel

from src.event_store import EventStore
from src.commands.handlers import CommandHandlers
from src.mcp.tools import MCPTools
from src.mcp.resources import MCPResources
from src.upcasting.registry import UpcasterRegistry
from src.upcasting import upcasters  # Import to register upcasters

app = FastAPI(title="The Ledger - Agentic Event Store")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global instances
event_store: Optional[EventStore] = None
command_handlers: Optional[CommandHandlers] = None
mcp_tools: Optional[MCPTools] = None
mcp_resources: Optional[MCPResources] = None

class ToolCallRequest(BaseModel):
    """Request model for tool calls"""
    tool_name: str
    arguments: Dict[str, Any]
    correlation_id: Optional[str] = None

class ResourceRequest(BaseModel):
    """Request model for resource queries"""
    uri: str
    parameters: Optional[Dict[str, Any]] = None

@app.on_event("startup")
async def startup_event():
    """Initialize database connection and services on startup"""
    global event_store, command_handlers, mcp_tools, mcp_resources
    
    # Create database pool
    pool = await asyncpg.create_pool(
        "postgresql://user:password@localhost/ledger",
        min_size=10,
        max_size=20
    )
    
    # Create upcaster registry
    registry = UpcasterRegistry()
    
    # Initialize event store
    event_store = EventStore(pool, registry)
    
    # Initialize command handlers
    command_handlers = CommandHandlers(event_store)
    
    # Initialize MCP components
    mcp_tools = MCPTools(command_handlers)
    mcp_resources = MCPResources(event_store)

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on shutdown"""
    if event_store and event_store.pool:
        await event_store.pool.close()

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "The Ledger"}

@app.post("/mcp/tool")
async def call_tool(request: ToolCallRequest):
    """
    MCP Tool endpoint - Command side.
    LLM agents call tools to perform actions that append events.
    """
    try:
        result = await mcp_tools.execute_tool(
            request.tool_name,
            request.arguments,
            request.correlation_id
        )
        return {
            "success": True,
            "result": result,
            "error": None
        }
    except Exception as e:
        # Return structured error for LLM consumption
        return {
            "success": False,
            "result": None,
            "error": {
                "type": e.__class__.__name__,
                "message": str(e),
                "suggested_action": getattr(e, "suggested_action", None)
            }
        }

@app.post("/mcp/resource")
async def get_resource(request: ResourceRequest):
    """
    MCP Resource endpoint - Query side.
    Read-only queries against projections.
    """
    try:
        data = await mcp_resources.get_resource(request.uri, request.parameters)
        return {
            "success": True,
            "data": data,
            "error": None
        }
    except Exception as e:
        return {
            "success": False,
            "data": None,
            "error": {
                "type": e.__class__.__name__,
                "message": str(e)
            }
        }

@app.get("/mcp/resources/{resource_path:path}")
async def get_resource_by_path(resource_path: str):
    """REST-style resource access"""
    uri = f"ledger://{resource_path}"
    return await get_resource(ResourceRequest(uri=uri))