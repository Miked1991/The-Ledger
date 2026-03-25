# tests/test_gas_town.py
import pytest
import pytest_asyncio
import asyncio
from src.event_store import EventStore
from src.integrity.gas_town import reconstruct_agent_context, AgentContext
from src.aggregates.agent_session import AgentSessionAggregate
from src.models.events import AgentContextLoaded, AgentActionTaken

@pytest.mark.asyncio
async def test_reconstruct_agent_context(event_store: EventStore):
    """Test that agent context can be reconstructed from event store"""
    agent_id = "test-agent"
    session_id = "test-session"
    stream_id = f"agent-{agent_id}-{session_id}"
    
    # Create agent session with context loaded
    context_event = AgentContextLoaded(
        agent_id=agent_id,
        session_id=session_id,
        context_source="database",
        token_count=2048,
        model_version="v2.0",
        context_hash="hash123"
    )
    
    await event_store.append(stream_id, [context_event], expected_version=-1)
    
    # Add some actions
    actions = [
        AgentActionTaken(
            agent_id=agent_id,
            session_id=session_id,
            action_type="credit_analysis",
            input_data_hash="input1",
            reasoning_trace="Analyzed credit for application APP-001",
            output_data={"risk_tier": "MEDIUM", "score": 750}
        ),
        AgentActionTaken(
            agent_id=agent_id,
            session_id=session_id,
            action_type="fraud_check",
            input_data_hash="input2",
            reasoning_trace="Checked fraud indicators",
            output_data={"fraud_score": 0.12, "flags": []}
        ),
        AgentActionTaken(
            agent_id=agent_id,
            session_id=session_id,
            action_type="decision",
            input_data_hash="input3",
            reasoning_trace="Generated recommendation",
            output_data={"recommendation": "APPROVE", "confidence": 0.85}
        )
    ]
    
    for action in actions:
        await event_store.append(
            stream_id,
            [action],
            expected_version=await event_store.stream_version(stream_id)
        )
    
    # Simulate crash - reconstruct context without in-memory object
    context = await reconstruct_agent_context(event_store, agent_id, session_id)
    
    # Verify reconstruction
    assert isinstance(context, AgentContext)
    assert context.last_event_position == 4  # 1 context + 3 actions
    assert "Session test-session recovery" in context.context_text
    assert "Context loaded: True" in context.context_text
    
    # Verify pending work detection
    # No pending work since all actions completed
    assert len(context.pending_work) == 0
    assert context.session_health_status == "HEALTHY"
    
    print("✅ Agent context reconstruction test passed")

@pytest.mark.asyncio
async def test_gas_town_partial_decision(event_store: EventStore):
    """
    Test that partial decisions are flagged as NEEDS_RECONCILIATION.
    This tests the Gas Town pattern requirement.
    """
    agent_id = "test-agent-partial"
    session_id = "test-session-partial"
    stream_id = f"agent-{agent_id}-{session_id}"
    
    # Load context
    context_event = AgentContextLoaded(
        agent_id=agent_id,
        session_id=session_id,
        context_source="database",
        token_count=1024,
        model_version="v2.0",
        context_hash="hash456"
    )
    
    await event_store.append(stream_id, [context_event], expected_version=-1)
    
    # Add a partial decision (incomplete action)
    partial_action = AgentActionTaken(
        agent_id=agent_id,
        session_id=session_id,
        action_type="decision",
        input_data_hash="partial_input",
        reasoning_trace="Partial decision - crashed during processing",
        output_data={"partial": True}
    )
    
    await event_store.append(
        stream_id,
        [partial_action],
        expected_version=await event_store.stream_version(stream_id)
    )
    
    # Reconstruct context
    context = await reconstruct_agent_context(event_store, agent_id, session_id)
    
    # Should detect partial decision as pending work
    assert len(context.pending_work) > 0
    assert context.pending_work[0]["type"] == "decision"
    assert context.session_health_status == "NEEDS_RECONCILIATION"
    assert "PENDING" in context.context_text
    
    print("✅ Gas Town partial decision detection test passed")

@pytest.mark.asyncio
async def test_gas_town_crash_recovery(event_store: EventStore):
    """
    Simulate a crash and verify the agent can continue with reconstructed context.
    """
    agent_id = "test-agent-recovery"
    session_id = "test-session-recovery"
    stream_id = f"agent-{agent_id}-{session_id}"
    
    # Initial session setup
    agg = await AgentSessionAggregate.load(event_store, agent_id, session_id)
    
    context_event = agg.create_context_loaded_event(
        context_source="database",
        model_version="v2.0",
        token_count=2048,
        context_hash="hash789"
    )
    agg.apply_new_event(context_event)
    
    await agg.commit(
        event_store,
        stream_id,
        expected_version=agg.version,
        correlation_id="test-correlation"
    )
    
    # Add some completed work
    action1 = agg.create_action_taken_event(
        action_type="credit_analysis",
        input_hash="hash1",
        reasoning_trace="Completed analysis",
        output_data={"status": "completed"}
    )
    agg.apply_new_event(action1)
    await agg.commit(event_store, stream_id, agg.version)
    
    # Simulate crash - no in-memory aggregate
    
    # Reconstruct context from store
    context = await reconstruct_agent_context(event_store, agent_id, session_id)
    
    # Verify context contains enough information to continue
    assert "credit_analysis" in context.context_text
    assert context.last_event_position >= 2
    
    # Create new aggregate and apply reconstructed state
    new_agg = await AgentSessionAggregate.load(event_store, agent_id, session_id)
    new_agg.assert_context_loaded()  # Should not raise
    
    # Continue with new work
    action2 = new_agg.create_action_taken_event(
        action_type="fraud_check",
        input_hash="hash2",
        reasoning_trace="Continued after crash",
        output_data={"status": "completed", "resumed": True}
    )
    new_agg.apply_new_event(action2)
    
    await new_agg.commit(
        event_store,
        stream_id,
        expected_version=new_agg.version,
        correlation_id="test-correlation"
    )
    
    # Verify final state
    final_agg = await AgentSessionAggregate.load(event_store, agent_id, session_id)
    assert len(final_agg.actions_taken) == 2
    
    print("✅ Gas Town crash recovery test passed")

@pytest.mark.asyncio
async def test_gas_town_token_budget(event_store: EventStore):
    """Test that token budget is respected in context reconstruction"""
    agent_id = "test-agent-budget"
    session_id = "test-session-budget"
    stream_id = f"agent-{agent_id}-{session_id}"
    
    # Create session with many events
    context_event = AgentContextLoaded(
        agent_id=agent_id,
        session_id=session_id,
        context_source="database",
        token_count=10000,
        model_version="v2.0",
        context_hash="hash"
    )
    await event_store.append(stream_id, [context_event], expected_version=-1)
    
    # Add many actions
    for i in range(100):
        action = AgentActionTaken(
            agent_id=agent_id,
            session_id=session_id,
            action_type=f"action_{i}",
            input_data_hash=f"hash_{i}",
            reasoning_trace=f"Reasoning for action {i}" * 10,
            output_data={"index": i}
        )
        await event_store.append(
            stream_id,
            [action],
            expected_version=await event_store.stream_version(stream_id)
        )
    
    # Reconstruct with small token budget
    small_budget = 500
    context_small = await reconstruct_agent_context(
        event_store, agent_id, session_id, token_budget=small_budget
    )
    
    # Reconstruct with large budget
    context_large = await reconstruct_agent_context(
        event_store, agent_id, session_id, token_budget=50000
    )
    
    # Small budget should truncate context
    assert len(context_small.context_text) <= small_budget + 100  # Allow some overhead
    assert len(context_large.context_text) > len(context_small.context_text)
    
    print("✅ Gas Town token budget test passed")

@pytest.mark.asyncio
async def test_gas_town_empty_session(event_store: EventStore):
    """Test reconstructing context for non-existent session"""
    agent_id = "nonexistent-agent"
    session_id = "nonexistent-session"
    
    context = await reconstruct_agent_context(event_store, agent_id, session_id)
    
    assert context.session_health_status == "NEW"
    assert context.last_event_position == 0
    assert len(context.pending_work) == 0
    assert "No existing session" in context.context_text
    
    print("✅ Gas Town empty session test passed")