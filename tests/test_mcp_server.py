# tests/test_mcp_server.py
"""
Comprehensive tests for MCP server endpoints.
"""

import pytest
import pytest_asyncio
import httpx
import asyncio
from fastapi.testclient import TestClient
from src.mcp.server import app


@pytest_asyncio.fixture
def client():
    """Create test client for MCP server"""
    return TestClient(app)


@pytest.mark.asyncio
async def test_health_check(client):
    """Test health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] in ["healthy", "degraded"]
    assert data["version"] == "1.0.0"
    assert "timestamp" in data
    assert "components" in data


@pytest.mark.asyncio
async def test_submit_application_tool(client):
    """Test submit_application tool"""
    response = client.post("/mcp/tool", json={
        "tool_name": "submit_application",
        "arguments": {
            "application_id": "MCP-TEST-001",
            "applicant_id": "test-applicant",
            "requested_amount": 500000,
            "business_name": "MCP Test Corp",
            "tax_id": "12-3456789"
        },
        "correlation_id": "test-correlation-1"
    })
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "stream_id" in data["result"]
    assert "loan-MCP-TEST-001" in data["result"]["stream_id"]


@pytest.mark.asyncio
async def test_start_agent_session_tool(client):
    """Test start_agent_session tool"""
    response = client.post("/mcp/tool", json={
        "tool_name": "start_agent_session",
        "arguments": {
            "agent_id": "test-agent-001",
            "session_id": "test-session-001",
            "context_source": "database",
            "model_version": "v2.0",
            "token_count": 2048,
            "context_hash": "test_hash_123"
        },
        "correlation_id": "test-correlation-2"
    })
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["result"]["session_id"] == "test-session-001"
    assert "next_steps" in data["result"]


@pytest.mark.asyncio
async def test_complete_lifecycle_mcp_only(client):
    """
    Complete loan application lifecycle driven entirely through MCP tools.
    This tests the full integration of command and query sides.
    """
    application_id = "MCP-LIFECYCLE-001"
    agent_id = "test-agent-001"
    session_id = "test-session-001"
    
    # Step 1: Start agent session
    response = client.post("/mcp/tool", json={
        "tool_name": "start_agent_session",
        "arguments": {
            "agent_id": agent_id,
            "session_id": session_id,
            "context_source": "database",
            "model_version": "v2.0",
            "token_count": 2048,
            "context_hash": "test_hash"
        }
    })
    assert response.json()["success"] is True
    
    # Step 2: Submit application
    response = client.post("/mcp/tool", json={
        "tool_name": "submit_application",
        "arguments": {
            "application_id": application_id,
            "applicant_id": "test-applicant",
            "requested_amount": 500000,
            "business_name": "Test Corp",
            "tax_id": "12-3456789"
        }
    })
    assert response.json()["success"] is True
    
    # Step 3: Record credit analysis
    response = client.post("/mcp/tool", json={
        "tool_name": "record_credit_analysis",
        "arguments": {
            "application_id": application_id,
            "agent_id": agent_id,
            "session_id": session_id,
            "risk_tier": "MEDIUM",
            "credit_score": 720,
            "max_credit_limit": 450000,
            "model_version": "v2.0",
            "confidence_score": 0.82
        }
    })
    assert response.json()["success"] is True
    
    # Step 4: Record fraud screening
    response = client.post("/mcp/tool", json={
        "tool_name": "record_fraud_screening",
        "arguments": {
            "application_id": application_id,
            "agent_id": agent_id,
            "session_id": session_id,
            "fraud_score": 0.05,
            "flags": [],
            "risk_indicators": {}
        }
    })
    assert response.json()["success"] is True
    
    # Step 5: Record compliance check
    response = client.post("/mcp/tool", json={
        "tool_name": "record_compliance_check",
        "arguments": {
            "application_id": application_id,
            "rule_id": "BASEL_III_2024",
            "regulation_version": "v2.0",
            "check_id": "check-001",
            "passed": True,
            "details": {"verified": True}
        }
    })
    assert response.json()["success"] is True
    
    # Step 6: Generate decision
    response = client.post("/mcp/tool", json={
        "tool_name": "generate_decision",
        "arguments": {
            "application_id": application_id,
            "agent_id": agent_id,
            "session_id": session_id,
            "orchestrator_recommendation": "APPROVE",
            "orchestrator_confidence": 0.88,
            "contributing_sessions": [session_id],
            "reasoning": "All criteria met"
        }
    })
    assert response.json()["success"] is True
    
    # Step 7: Record human review
    response = client.post("/mcp/tool", json={
        "tool_name": "record_human_review",
        "arguments": {
            "application_id": application_id,
            "reviewer_id": "reviewer-001",
            "final_decision": "APPROVED",
            "override": False,
            "comments": "Approved"
        }
    })
    assert response.json()["success"] is True
    
    # Step 8: Query application status (Resource)
    response = client.get(f"/mcp/resources/applications/{application_id}/status")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["data"]["application_id"] == application_id
    assert data["data"]["status"] == "FINAL_APPROVED"
    
    # Step 9: Get compliance audit
    response = client.get(f"/mcp/resources/applications/{application_id}/compliance")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["data"]["application_id"] == application_id
    
    # Step 10: Get audit trail
    response = client.get(f"/mcp/resources/applications/{application_id}/audit-trail")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["data"]["total_events"] >= 7
    
    # Step 11: Run integrity check
    response = client.post("/mcp/tool", json={
        "tool_name": "run_integrity_check",
        "arguments": {
            "entity_type": "loan",
            "entity_id": application_id
        }
    })
    assert response.json()["success"] is True
    result = response.json()["result"]
    assert result["chain_valid"] is True
    assert result["tamper_detected"] is False


@pytest.mark.asyncio
async def test_resource_temporal_query(client):
    """Test temporal query on compliance audit"""
    application_id = "TEMP-QUERY-001"
    
    # Submit application
    client.post("/mcp/tool", json={
        "tool_name": "submit_application",
        "arguments": {
            "application_id": application_id,
            "applicant_id": "test",
            "requested_amount": 100000,
            "business_name": "Test",
            "tax_id": "12-3456789"
        }
    })
    
    # Query with temporal parameter
    response = client.get(
        f"/mcp/resources/applications/{application_id}/compliance",
        params={"as_of": "2024-01-01T00:00:00Z"}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    # Should return state as of that date (which may be empty or default)


@pytest.mark.asyncio
async def test_error_handling_structured(client):
    """Test structured error handling for LLM consumption"""
    
    # Try to generate decision without active session
    response = client.post("/mcp/tool", json={
        "tool_name": "generate_decision",
        "arguments": {
            "application_id": "nonexistent",
            "agent_id": "test",
            "session_id": "nonexistent",
            "orchestrator_recommendation": "APPROVE",
            "orchestrator_confidence": 0.85,
            "contributing_sessions": [],
            "reasoning": "Test"
        }
    })
    
    data = response.json()
    assert data["success"] is False
    assert data["error"] is not None
    assert data["error"]["type"] is not None
    assert "suggested_action" in data["error"] or "suggested_next_steps" in data


@pytest.mark.asyncio
async def test_stats_endpoint(client):
    """Test stats endpoint"""
    response = client.get("/stats")
    assert response.status_code == 200
    
    data = response.json()
    assert "event_store" in data
    assert "projection_daemon" in data
    assert "mcp_tools" in data


@pytest.mark.asyncio
async def test_lags_endpoint(client):
    """Test projection lags endpoint"""
    response = client.get("/lags")
    assert response.status_code == 200
    
    data = response.json()
    # May be empty if no projections initialized
    assert isinstance(data, dict)