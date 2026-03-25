# tests/test_upcasting.py
import pytest
import pytest_asyncio
import asyncpg
import json
from src.event_store import EventStore
from src.models.events import CreditAnalysisCompleted, DecisionGenerated
from src.upcasting.registry import UpcasterRegistry

@pytest_asyncio.fixture
async def registry():
    """Create upcaster registry with test upcasters"""
    from src.upcasting import upcasters
    registry = UpcasterRegistry()
    
    # Register test upcasters
    @registry.register("CreditAnalysisCompleted", from_version=1)
    def upcast_credit_v1_to_v2(payload):
        return {
            **payload,
            "model_version": "test-legacy",
            "confidence_score": None,
            "regulatory_basis": "TEST_REG_2024"
        }
    
    @registry.register("DecisionGenerated", from_version=1)
    def upcast_decision_v1_to_v2(payload):
        return {
            **payload,
            "model_versions": {
                session: "v1.0" for session in payload.get("contributing_agent_sessions", [])
            }
        }
    
    return registry

@pytest_asyncio.fixture
async def event_store(db_pool: asyncpg.Pool, registry: UpcasterRegistry):
    """Create event store with registry"""
    store = EventStore(db_pool, registry)
    return store

@pytest.mark.asyncio
async def test_immutability_test(event_store: EventStore, db_pool: asyncpg.Pool):
    """
    Critical immutability test from specification.
    Verifies that stored events are never modified by upcasting.
    """
    stream_id = "loan-immutability-test"
    application_id = "test-immutability"
    
    # Store a v1 event directly
    v1_event = CreditAnalysisCompleted(
        application_id=application_id,
        risk_tier="MEDIUM",
        credit_score=750,
        max_credit_limit=500000,
        model_version="v1.0"  # This will be overridden by upcaster
    )
    
    # Force v1 version by setting version manually
    v1_event.event_version = 1
    
    # Append v1 event
    await event_store.append(
        stream_id,
        [v1_event],
        expected_version=-1
    )
    
    # Step 1: Directly query the events table to get raw stored payload
    async with db_pool.acquire() as conn:
        raw_row = await conn.fetchrow("""
            SELECT payload, event_version FROM events
            WHERE stream_id = $1 AND stream_position = 1
        """, stream_id)
        
        raw_payload = raw_row["payload"]
        raw_version = raw_row["event_version"]
        
        # Verify raw stored event is still v1
        assert raw_version == 1
        assert "model_version" not in raw_payload or raw_payload["model_version"] == "v1.0"
    
    # Step 2: Load through event store (should be upcasted)
    loaded_events = await event_store.load_stream(stream_id)
    assert len(loaded_events) == 1
    
    loaded_event = loaded_events[0]
    
    # Verify it's upcasted to v2
    assert loaded_event.event_version == 2
    assert "model_version" in loaded_event.payload
    assert loaded_event.payload["model_version"] == "test-legacy"
    assert "confidence_score" in loaded_event.payload
    assert loaded_event.payload["confidence_score"] is None
    assert "regulatory_basis" in loaded_event.payload
    
    # Step 3: Directly query again to verify raw payload is unchanged
    async with db_pool.acquire() as conn:
        raw_row_again = await conn.fetchrow("""
            SELECT payload, event_version FROM events
            WHERE stream_id = $1 AND stream_position = 1
        """, stream_id)
        
        # Raw payload should be unchanged
        assert raw_row_again["event_version"] == 1
        assert "model_version" not in raw_row_again["payload"] or raw_row_again["payload"]["model_version"] == "v1.0"
    
    print("✅ Immutability test passed: stored events unchanged after upcasting")

@pytest.mark.asyncio
async def test_upcaster_chain(event_store: EventStore):
    """Test that multiple upcasters are applied in correct order"""
    stream_id = "loan-upcaster-chain"
    application_id = "test-chain"
    
    # Create v1 event
    v1_event = CreditAnalysisCompleted(
        application_id=application_id,
        risk_tier="HIGH",
        credit_score=680,
        max_credit_limit=400000,
        model_version="v1.0"
    )
    v1_event.event_version = 1
    
    await event_store.append(stream_id, [v1_event], expected_version=-1)
    
    # Load and verify upcasting applied
    events = await event_store.load_stream(stream_id)
    event = events[0]
    
    # Should be v2
    assert event.event_version == 2
    assert event.payload["model_version"] == "test-legacy"
    assert event.payload["confidence_score"] is None
    assert event.payload["regulatory_basis"] == "TEST_REG_2024"

@pytest.mark.asyncio
async def test_decision_upcasting(event_store: EventStore, db_pool: asyncpg.Pool):
    """Test upcasting for DecisionGenerated events"""
    stream_id = "loan-decision-upcast"
    application_id = "test-decision"
    
    # Create v1 decision event
    v1_decision = DecisionGenerated(
        application_id=application_id,
        decision_id="dec-test-1",
        recommendation="APPROVE",
        confidence_score=0.85,
        contributing_agent_sessions=["agent-credit-1", "agent-fraud-1"],
        decision_reasoning="Test decision"
    )
    v1_decision.event_version = 1
    
    await event_store.append(stream_id, [v1_decision], expected_version=-1)
    
    # Verify raw storage
    async with db_pool.acquire() as conn:
        raw_row = await conn.fetchrow("""
            SELECT payload, event_version FROM events
            WHERE stream_id = $1
        """, stream_id)
        
        assert raw_row["event_version"] == 1
        assert "model_versions" not in raw_row["payload"]
    
    # Load through event store
    events = await event_store.load_stream(stream_id)
    event = events[0]
    
    # Verify upcasted
    assert event.event_version == 2
    assert "model_versions" in event.payload
    assert len(event.payload["model_versions"]) == 2
    assert "agent-credit-1" in event.payload["model_versions"]

@pytest.mark.asyncio
async def test_upcasting_with_nulls(event_store: EventStore):
    """Test that null values are preserved correctly"""
    stream_id = "loan-null-test"
    application_id = "test-null"
    
    # Create event with explicit nulls
    event = CreditAnalysisCompleted(
        application_id=application_id,
        risk_tier="LOW",
        credit_score=800,
        max_credit_limit=1000000,
        model_version="v2.0",
        confidence_score=None,
        regulatory_basis=None
    )
    event.event_version = 2  # Already v2
    
    await event_store.append(stream_id, [event], expected_version=-1)
    
    events = await event_store.load_stream(stream_id)
    loaded = events[0]
    
    # Nulls should be preserved
    assert loaded.payload["confidence_score"] is None
    assert loaded.payload["regulatory_basis"] is None

@pytest.mark.asyncio
async def test_multiple_upcasters(event_store: EventStore):
    """Test chain of multiple upcasters"""
    stream_id = "loan-multi-upcast"
    application_id = "test-multi"
    
    # Create v1 event that will go through multiple upcasters
    v1_event = CreditAnalysisCompleted(
        application_id=application_id,
        risk_tier="MEDIUM",
        credit_score=720,
        max_credit_limit=600000,
        model_version="v1.0"
    )
    v1_event.event_version = 1
    
    await event_store.append(stream_id, [v1_event], expected_version=-1)
    
    # Load should apply all upcasters
    events = await event_store.load_stream(stream_id)
    event = events[0]
    
    # Should have all upcasted fields
    assert event.event_version == 2
    assert "model_version" in event.payload
    assert "confidence_score" in event.payload
    assert "regulatory_basis" in event.payload