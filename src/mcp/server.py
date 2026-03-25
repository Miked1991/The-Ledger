# src/mcp/server.py
"""
MCP Server - Exposes The Ledger as enterprise infrastructure for AI agents.
Implements CQRS with tools (commands) and resources (queries).
"""

import asyncio
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import asyncpg

from src.event_store import EventStore
from src.commands.handlers import CommandHandlers
from src.mcp.tools import MCPTools
from src.mcp.resources import MCPResources
from src.projections.daemon import ProjectionDaemon
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.agent_performance import AgentPerformanceProjection
from src.projections.compliance_audit import ComplianceAuditProjection
from src.upcasting.registry import UpcasterRegistry
from src.upcasting import upcasters  # Import to register upcasters

logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(
    title="The Ledger - Agentic Event Store",
    description="MCP Server for immutable event sourcing infrastructure",
    version="1.0.0"
)

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
projection_daemon: Optional[ProjectionDaemon] = None

# WebSocket connections
active_websockets: Dict[str, List[WebSocket]] = {}


# Request/Response Models
class ToolCallRequest(BaseModel):
    """Request model for tool calls"""
    tool_name: str = Field(..., description="Name of the tool to call")
    arguments: Dict[str, Any] = Field(default_factory=dict, description="Tool arguments")
    correlation_id: Optional[str] = Field(None, description="Correlation ID for tracing")


class ToolCallResponse(BaseModel):
    """Response model for tool calls"""
    success: bool = Field(..., description="Whether the call succeeded")
    result: Optional[Dict[str, Any]] = Field(None, description="Tool result if successful")
    error: Optional[Dict[str, Any]] = Field(None, description="Error details if failed")
    correlation_id: Optional[str] = Field(None, description="Correlation ID")


class ResourceRequest(BaseModel):
    """Request model for resource queries"""
    uri: str = Field(..., description="Resource URI")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Query parameters")


class ResourceResponse(BaseModel):
    """Response model for resource queries"""
    success: bool = Field(..., description="Whether the query succeeded")
    data: Optional[Any] = Field(None, description="Resource data if successful")
    error: Optional[Dict[str, Any]] = Field(None, description="Error details if failed")


class HealthResponse(BaseModel):
    """Health check response"""
    status: str = Field(..., description="System status")
    version: str = Field(..., description="System version")
    timestamp: str = Field(..., description="Current timestamp")
    components: Dict[str, Any] = Field(default_factory=dict, description="Component status")


@app.on_event("startup")
async def startup_event():
    """Initialize database connection and services on startup"""
    global event_store, command_handlers, mcp_tools, mcp_resources, projection_daemon
    
    logger.info("Starting MCP Server...")
    
    # Create database pool
    pool = await asyncpg.create_pool(
        "postgresql://postgres:postgres@localhost:5432/ledger",
        min_size=10,
        max_size=20,
        command_timeout=60
    )
    logger.info("Database pool created")
    
    # Create upcaster registry
    registry = UpcasterRegistry()
    
    # Initialize event store
    event_store = EventStore(pool, registry)
    logger.info("Event store initialized")
    
    # Initialize command handlers
    command_handlers = CommandHandlers(event_store)
    logger.info("Command handlers initialized")
    
    # Initialize projections
    app_summary = ApplicationSummaryProjection()
    agent_perf = AgentPerformanceProjection()
    compliance = ComplianceAuditProjection()
    
    app_summary.set_pool(pool)
    agent_perf.set_pool(pool)
    compliance.set_pool(pool)
    
    app_summary.set_store(event_store)
    agent_perf.set_store(event_store)
    compliance.set_store(event_store)
    
    # Initialize projection daemon
    projection_daemon = ProjectionDaemon(
        event_store,
        [app_summary, agent_perf, compliance],
        max_retries=3,
        batch_size=100,
        poll_interval_ms=100
    )
    
    # Start daemon in background
    asyncio.create_task(projection_daemon.run_forever())
    logger.info("Projection daemon started")
    
    # Initialize MCP components
    mcp_tools = MCPTools(command_handlers)
    mcp_resources = MCPResources(event_store, projection_daemon)
    logger.info("MCP components initialized")
    
    logger.info("MCP Server ready")


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on shutdown"""
    global projection_daemon
    
    logger.info("Shutting down MCP Server...")
    
    if projection_daemon:
        projection_daemon.stop()
    
    if event_store and event_store.pool:
        await event_store.pool.close()
        logger.info("Database pool closed")


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    components = {
        "database": "unknown",
        "event_store": "unknown",
        "projection_daemon": "unknown"
    }
    
    # Check database
    if event_store and event_store.pool:
        try:
            async with event_store.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            components["database"] = "healthy"
        except Exception:
            components["database"] = "unhealthy"
    
    # Check event store
    if event_store:
        components["event_store"] = "healthy"
    else:
        components["event_store"] = "unhealthy"
    
    # Check projection daemon
    if projection_daemon and projection_daemon._running:
        components["projection_daemon"] = "healthy"
    else:
        components["projection_daemon"] = "unhealthy"
    
    status = "healthy" if all(v == "healthy" for v in components.values()) else "degraded"
    
    return HealthResponse(
        status=status,
        version="1.0.0",
        timestamp=datetime.utcnow().isoformat(),
        components=components
    )


@app.post("/mcp/tool", response_model=ToolCallResponse)
async def call_tool(request: ToolCallRequest):
    """
    MCP Tool endpoint - Command side.
    LLM agents call tools to perform actions that append events.
    """
    try:
        logger.info(f"Tool call: {request.tool_name} (correlation: {request.correlation_id})")
        
        result = await mcp_tools.execute_tool(
            request.tool_name,
            request.arguments,
            request.correlation_id
        )
        
        return ToolCallResponse(
            success=True,
            result=result,
            correlation_id=request.correlation_id
        )
        
    except Exception as e:
        logger.error(f"Tool call failed: {e}", exc_info=True)
        
        # Return structured error for LLM consumption
        error_response = {
            "type": e.__class__.__name__,
            "message": str(e),
            "suggested_action": getattr(e, "suggested_action", None)
        }
        
        return ToolCallResponse(
            success=False,
            error=error_response,
            correlation_id=request.correlation_id
        )


@app.post("/mcp/resource", response_model=ResourceResponse)
async def get_resource(request: ResourceRequest):
    """
    MCP Resource endpoint - Query side.
    Read-only queries against projections.
    """
    try:
        logger.info(f"Resource query: {request.uri}")
        
        data = await mcp_resources.get_resource(request.uri, request.parameters)
        
        return ResourceResponse(
            success=True,
            data=data
        )
        
    except Exception as e:
        logger.error(f"Resource query failed: {e}", exc_info=True)
        
        return ResourceResponse(
            success=False,
            error={
                "type": e.__class__.__name__,
                "message": str(e)
            }
        )


@app.get("/mcp/resources/{resource_path:path}")
async def get_resource_by_path(resource_path: str, as_of: Optional[str] = None):
    """REST-style resource access with temporal query support"""
    uri = f"ledger://{resource_path}"
    parameters = {}
    
    if as_of:
        parameters["as_of"] = as_of
    
    return await get_resource(ResourceRequest(uri=uri, parameters=parameters))


@app.websocket("/ws/{application_id}")
async def websocket_endpoint(websocket: WebSocket, application_id: str):
    """
    WebSocket endpoint for real-time updates.
    Used for optimistic UI pattern and push notifications.
    """
    await websocket.accept()
    
    # Register connection
    if application_id not in active_websockets:
        active_websockets[application_id] = []
    active_websockets[application_id].append(websocket)
    
    try:
        # Send initial state
        initial_state = await mcp_resources.get_application_status(application_id)
        await websocket.send_json({
            "type": "initial_state",
            "data": initial_state,
            "timestamp": datetime.utcnow().isoformat()
        })
        
        # Keep connection alive
        while True:
            # Receive ping/pong
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
                
    except WebSocketDisconnect:
        # Remove connection
        if application_id in active_websockets:
            active_websockets[application_id].remove(websocket)
            if not active_websockets[application_id]:
                del active_websockets[application_id]


@app.get("/stats")
async def get_stats():
    """Get system statistics"""
    stats = {
        "event_store": event_store.get_stats() if event_store else None,
        "projection_daemon": projection_daemon.get_stats() if projection_daemon else None,
        "mcp_tools": mcp_tools.get_stats() if mcp_tools else None,
        "active_websockets": {
            app_id: len(connections) 
            for app_id, connections in active_websockets.items()
        }
    }
    
    return JSONResponse(stats)


@app.get("/lags")
async def get_projection_lags():
    """Get projection lag metrics"""
    if not projection_daemon:
        return JSONResponse({"error": "Projection daemon not initialized"}, status_code=503)
    
    lags = await projection_daemon.get_all_lags()
    return JSONResponse(lags)


@app.post("/rebuild/{projection_name}")
async def rebuild_projection(projection_name: str):
    """Rebuild a projection from scratch"""
    if not projection_daemon:
        return JSONResponse({"error": "Projection daemon not initialized"}, status_code=503)
    
    try:
        await projection_daemon.rebuild_projection(projection_name)
        return JSONResponse({"status": "success", "projection": projection_name})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.get("/dead-letter")
async def get_dead_letter_queue():
    """Get dead letter queue contents"""
    if not projection_daemon:
        return JSONResponse({"error": "Projection daemon not initialized"}, status_code=503)
    
    queue = await projection_daemon.get_dead_letter_queue()
    return JSONResponse({"dead_letter_queue": queue, "size": len(queue)})


@app.post("/dead-letter/reprocess")
async def reprocess_dead_letter():
    """Reprocess events from dead letter queue"""
    if not projection_daemon:
        return JSONResponse({"error": "Projection daemon not initialized"}, status_code=503)
    
    await projection_daemon.reprocess_dead_letter()
    return JSONResponse({"status": "success"})