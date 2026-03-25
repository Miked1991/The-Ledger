# tests/test_mcp_lifecycle.py
import pytest
import pytest_asyncio
import httpx
from typing import Dict, Any
from src.mcp.server import app
from src.models.events import ApplicationState

@pytest_asyncio.fixture
async def client():
    """Create test client for MCP server"""
    from fastapi.testclient import TestClient
    return TestClient(app)

@pytest_asyncio.fixture
async def setup_test_data(client):
    """Setup test data before tests"""
    # This would create necessary database setup
    pass

@pytest.mark.asyncio
async def test_complete_loan_lifecycle_mcp_only(client):
    """
    Drive a complete loan application lifecycle using only MCP tool calls.
    This is the critical integration test from the specification.
    """
    application_id = "mcp-lifecycle-test-001"
    agent_id = "test-agent-001"
    session_id = "test-session-001"
    
    print("\n=== Starting MCP Lifecycle Test ===")
    
    # Step 1: Start agent session (Gas Town pattern)
    print("\n1. Starting agent session...")
    response = client.post("/mcp/tool", json={
        "tool_name": "start_agent_session",
        "arguments": {
            "agent_id": agent_id,
            "session_id": session_id,
            "context_source": "test_database",
            "model_version": "v2.0",
            "token_count": 2048,
            "context_hash": "test_hash_123"
        },
        "correlation_id": "test-correlation-1"
    })
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["result"]["session_id"] == session_id
    print(f"✓ Agent session started: {session_id}")
    
    # Step 2: Submit application
    print("\n2. Submitting application...")
    response = client.post("/mcp/tool", json={
        "tool_name": "submit_application",
        "arguments": {
            "application_id": application_id,
            "applicant_id": "test-applicant",
            "requested_amount": 500000,
            "business_name": "MCP Test Corp",
            "tax_id": "12-3456789"
        },
        "correlation_id": "test-correlation-2"
    })
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert application_id in data["result"]["stream_id"]
    print(f"✓ Application submitted: {application_id}")
    
    # Step 3: Record credit analysis
    print("\n3. Recording credit analysis...")
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
        },
        "correlation_id": "test-correlation-3"
    })
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    print(f"✓ Credit analysis recorded")
    
    # Step 4: Record fraud screening
    print("\n4. Recording fraud screening...")
    response = client.post("/mcp/tool", json={
        "tool_name": "record_fraud_screening",
        "arguments": {
            "application_id": application_id,
            "agent_id": agent_id,
            "session_id": session_id,
            "fraud_score": 0.05,
            "flags": [],
            "risk_indicators": {"suspicious_patterns": []}
        },
        "correlation_id": "test-correlation-4"
    })
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    print(f"✓ Fraud screening recorded")
    
    # Step 5: Record compliance checks
    print("\n5. Recording compliance checks...")
    response = client.post("/mcp/tool", json={
        "tool_name": "record_compliance_check",
        "arguments": {
            "application_id": application_id,
            "agent_id": agent_id,
            "session_id": session_id,
            "rule_id": "BASEL_III_2024",
            "regulation_version": "v1.0",
            "check_id": "check-001",
            "passed": True,
            "details": {"verified": True}
        },
        "correlation_id": "test-correlation-5"
    })
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    print(f"✓ Compliance checks recorded")
    
    # Step 6: Generate decision
    print("\n6. Generating decision...")
    response = client.post("/mcp/tool", json={
        "tool_name": "generate_decision",
        "arguments": {
            "application_id": application_id,
            "agent_id": agent_id,
            "session_id": session_id,
            "orchestrator_recommendation": "APPROVE",
            "orchestrator_confidence": 0.88,
            "contributing_sessions": [session_id],
            "reasoning": "Application meets all criteria for approval"
        },
        "correlation_id": "test-correlation-6"
    })
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    print(f"✓ Decision generated: {data['result']['final_recommendation']}")
    
    # Step 7: Record human review
    print("\n7. Recording human review...")
    response = client.post("/mcp/tool", json={
        "tool_name": "record_human_review",
        "arguments": {
            "application_id": application_id,
            "reviewer_id": "human-reviewer-001",
            "final_decision": "APPROVED",
            "override": False,
            "comments": "All checks passed, approving application"
        },
        "correlation_id": "test-correlation-7"
    })
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["result"]["final_decision"] == "APPROVED"
    print(f"✓ Human review recorded: APPROVED")
    
    # Step 8: Query compliance audit view
    print("\n8. Querying compliance audit...")
    response = client.get(f"/mcp/resources/applications/{application_id}/compliance")
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["data"]["application_id"] == application_id
    print(f"✓ Compliance audit available")
    
    # Step 9: Get complete audit trail
    print("\n9. Getting audit trail...")
    response = client.get(f"/mcp/resources/applications/{application_id}/audit-trail")
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert len(data["data"]["events"]) >= 7  # At least 7 events in the lifecycle
    print(f"✓ Audit trail has {len(data['data']['events'])} events")
    
    # Step 10: Verify complete trace is present
    print("\n10. Verifying complete trace...")
    events = data["data"]["events"]
    event_types = [e["type"] for e in events]
    
    expected_events = [
        "AgentContextLoaded",
        "ApplicationSubmitted", 
        "CreditAnalysisCompleted",
        "FraudScreeningCompleted",
        "ComplianceRulePassed",
        "DecisionGenerated",
        "HumanReviewCompleted"
    ]
    
    for expected in expected_events:
        assert expected in event_types, f"Missing event: {expected}"
    
    print(f"✓ All expected events present: {expected_events}")
    
    # Step 11: Run integrity check
    print("\n11. Running integrity check...")
    response = client.post("/mcp/tool", json={
        "tool_name": "run_integrity_check",
        "arguments": {
            "entity_type": "loan",
            "entity_id": application_id
        },
        "correlation_id": "test-correlation-8"
    })
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["result"]["chain_valid"] is True
    assert data["result"]["tamper_detected"] is False
    print(f"✓ Integrity check passed: verified {data['result']['events_verified']} events")
    
    print("\n=== MCP Lifecycle Test COMPLETED SUCCESSFULLY ===")
    print("✅ All 11 steps passed - complete loan lifecycle driven entirely through MCP tools")

@pytest.mark.asyncio
async def test_mcp_resource_temporal_query(client):
    """Test temporal queries on compliance audit view"""
    application_id = "temporal-test-001"
    
    # Submit application first
    response = client.post("/mcp/tool", json={
        "tool_name": "start_agent_session",
        "arguments": {
            "agent_id": "test-agent",
            "session_id": "test-session",
            "context_source": "test",
            "model_version": "v1.0",
            "token_count": 1000,
            "context_hash": "hash"
        }
    })
    
    response = client.post("/mcp/tool", json={
        "tool_name": "submit_application",
        "arguments": {
            "application_id": application_id,
            "applicant_id": "test-applicant",
            "requested_amount": 100000,
            "business_name": "Test Corp",
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
    
    print("✅ Temporal query test passed")

@pytest.mark.asyncio
async def test_mcp_error_handling(client):
    """Test structured error handling for LLM consumption"""
    
    # Try to generate decision without active session
    response = client.post("/mcp/tool", json={
        "tool_name": "generate_decision",
        "arguments": {
            "application_id": "nonexistent",
            "agent_id": "test-agent",
            "session_id": "nonexistent-session",
            "orchestrator_recommendation": "APPROVE",
            "orchestrator_confidence": 0.85,
            "contributing_sessions": [],
            "reasoning": "Test"
        }
    })
    
    assert response.status_code == 200  # Tool endpoint always returns 200 with error field
    data = response.json()
    assert data["success"] is False
    assert data["error"] is not None
    assert data["error"]["type"] is not None
    assert "suggested_action" in data["error"] or "message" in data["error"]
    
    print("✅ Structured error handling test passed")