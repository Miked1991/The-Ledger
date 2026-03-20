"""
tests/test_concurrency.py — Double-Decision Concurrency Test
=============================================================
Critical test for optimistic concurrency control.

Tests that when two agents simultaneously append to the same stream:
- Exactly one succeeds
- One receives OptimisticConcurrencyError
- Stream ends up with correct number of events (initial + 1)
- No events are lost or duplicated

Also includes:
- Retry flow test
- High-contention simulation
- Causal consistency test
"""

import pytest
import asyncio
from datetime import datetime
from uuid import uuid4
from decimal import Decimal

from ledger.event_store import InMemoryEventStore, OptimisticConcurrencyError
from ledger.commands.handlers import CommandHandlers
from ledger.schema.events import (
    ApplicationSubmitted,
    CreditAnalysisCompleted,
    CreditDecision,
    RiskTier,
    AgentType
)


@pytest.fixture
async def event_store():
    """Fixture providing clean in-memory event store for each test"""
    store = InMemoryEventStore()
    # No need to connect - in-memory store is ready immediately
    yield store
    # No need to close - in-memory store has no connection


@pytest.fixture
async def handlers(event_store):
    """Fixture providing command handlers"""
    return CommandHandlers(event_store)


@pytest.mark.asyncio
async def test_double_decision_concurrency(event_store):
    """
    CRITICAL TEST: Two agents simultaneously append to same stream at expected_version=3.
    
    This tests the core optimistic concurrency control mechanism.
    
    Scenario:
    1. Create stream with 3 existing events (version = 3)
    2. Two agents both read version = 3
    3. Both attempt to append with expected_version = 3
    4. Exactly one must succeed, one must fail with OptimisticConcurrencyError
    5. Final stream must have 4 events (3 original + 1 new)
    """
    print("\n" + "="*60)
    print("TEST: Double-Decision Concurrency")
    print("="*60)
    
    stream_id = f"loan-concurrency-test-{uuid4()}"
    
    # 1. Create stream with 3 initial events (so version = 3)
    print(f"\n1. Creating stream {stream_id} with 3 initial events...")
    
    # Event 1: ApplicationSubmitted
    event1 = ApplicationSubmitted(
        event_type="ApplicationSubmitted",
        event_version=1,
        event_id=uuid4(),
        recorded_at=datetime.utcnow(),
        application_id="test-app-1",
        applicant_id="CUST-001",
        requested_amount_usd=Decimal("500000"),
        loan_purpose="expansion",
        loan_term_months=60,
        submission_channel="web",
        contact_email="test@example.com",
        contact_name="Test User",
        application_reference="REF-001"
    )
    
    positions = await event_store.append(
        stream_id=stream_id,
        events=[event1.to_store_dict()],
        expected_version=-1  # New stream
    )
    assert positions[0] == 0  # First event at position 0
    print(f"   Event 1 appended at position {positions[0]}")
    
    # Event 2: Another event to reach version 1
    event2 = ApplicationSubmitted(
        event_type="ApplicationSubmitted",
        event_version=1,
        event_id=uuid4(),
        recorded_at=datetime.utcnow(),
        application_id="test-app-2",
        applicant_id="CUST-002",
        requested_amount_usd=Decimal("250000"),
        loan_purpose="working_capital",
        loan_term_months=36,
        submission_channel="mobile",
        contact_email="test2@example.com",
        contact_name="Test User 2",
        application_reference="REF-002"
    )
    
    positions = await event_store.append(
        stream_id=stream_id,
        events=[event2.to_store_dict()],
        expected_version=0  # After first event
    )
    assert positions[0] == 1
    print(f"   Event 2 appended at position {positions[0]}")
    
    # Event 3: Another event to reach version 2
    event3 = ApplicationSubmitted(
        event_type="ApplicationSubmitted",
        event_version=1,
        event_id=uuid4(),
        recorded_at=datetime.utcnow(),
        application_id="test-app-3",
        applicant_id="CUST-003",
        requested_amount_usd=Decimal("750000"),
        loan_purpose="acquisition",
        loan_term_months=84,
        submission_channel="web",
        contact_email="test3@example.com",
        contact_name="Test User 3",
        application_reference="REF-003"
    )
    
    positions = await event_store.append(
        stream_id=stream_id,
        events=[event3.to_store_dict()],
        expected_version=1  # After second event
    )
    assert positions[0] == 2
    print(f"   Event 3 appended at position {positions[0]}")
    
    # Verify current version = 3 (0-based: events at positions 0,1,2)
    current_version = await event_store.stream_version(stream_id)
    assert current_version == 2  # 0-based, so 3 events = version 2
    print(f"\n2. Stream now has 3 events (version = {current_version})")
    
    # 2. Define agent task that tries to append with expected_version=2 (3rd event)
    async def agent_task(agent_id: str, delay_ms: float = 0):
        """Simulate an agent trying to append a credit analysis"""
        try:
            # Small delay to simulate processing time and increase collision chance
            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000)
            
            # Create credit decision
            decision = CreditDecision(
                risk_tier=RiskTier.MEDIUM,
                recommended_limit_usd=Decimal("400000"),
                confidence=0.85,
                rationale=f"Analysis by {agent_id}",
                key_concerns=[],
                data_quality_caveats=[],
                policy_overrides_applied=[]
            )
            
            # Create event
            event = CreditAnalysisCompleted(
                event_type="CreditAnalysisCompleted",
                event_version=2,
                event_id=uuid4(),
                recorded_at=datetime.utcnow(),
                application_id="test-app-1",
                session_id=f"session-{agent_id}",
                decision=decision,
                model_version="credit-model-v2",
                model_deployment_id="deploy-123",
                input_data_hash="hash123",
                analysis_duration_ms=150,
                regulatory_basis=["reg-2026-01"],
                completed_at=datetime.utcnow()
            )
            
            # Attempt to append with expected_version=2 (both agents use same version)
            positions = await event_store.append(
                stream_id=stream_id,
                events=[event.to_store_dict()],
                expected_version=2,  # Both agents think current version is 2
                causation_id=f"cause-{agent_id}",
                metadata={"agent_id": agent_id}
            )
            
            return {
                "success": True,
                "agent": agent_id,
                "position": positions[0] if positions else None
            }
            
        except OptimisticConcurrencyError as e:
            return {
                "success": False,
                "agent": agent_id,
                "error": e,
                "expected": e.expected,
                "actual": e.actual
            }
    
    # 3. Run both agents concurrently
    print("\n3. Launching two agents concurrently, both expecting version 2...")
    
    # Add small delay to first agent to ensure they both read before any write
    # This simulates both agents loading the aggregate at the same time
    task1 = asyncio.create_task(agent_task("agent-A", delay_ms=10))
    task2 = asyncio.create_task(agent_task("agent-B", delay_ms=10))
    
    results = await asyncio.gather(task1, task2)
    
    # 4. Analyze results
    print("\n4. Results:")
    successes = [r for r in results if r["success"]]
    failures = [r for r in results if not r["success"]]
    
    for r in results:
        if r["success"]:
            print(f"   ✓ {r['agent']} SUCCEEDED at position {r['position']}")
        else:
            print(f"   ✗ {r['agent']} FAILED: {r['error']}")
    
    # 5. Assertions
    print("\n5. Assertions:")
    
    # Exactly one success
    assert len(successes) == 1, f"Expected 1 success, got {len(successes)}"
    print(f"   ✓ Exactly one agent succeeded")
    
    # Exactly one failure
    assert len(failures) == 1, f"Expected 1 failure, got {len(failures)}"
    print(f"   ✓ Exactly one agent failed with OptimisticConcurrencyError")
    
    # Failure should be OptimisticConcurrencyError
    assert isinstance(failures[0]["error"], OptimisticConcurrencyError)
    print(f"   ✓ Failure is correct error type")
    
    # Error should have correct expected/actual
    assert failures[0]["expected"] == 2
    assert failures[0]["actual"] == 3  # After first agent appended
    print(f"   ✓ Error shows expected=2, actual=3")
    
    # Final stream should have 4 events (3 original + 1 new)
    final_events = await event_store.load_stream(stream_id)
    final_version = await event_store.stream_version(stream_id)
    
    assert len(final_events) == 4, f"Expected 4 events, got {len(final_events)}"
    print(f"   ✓ Final stream has 4 events (3 original + 1 new)")
    
    assert final_version == 3, f"Expected version 3, got {final_version}"
    print(f"   ✓ Final version = 3")
    
    # Verify the successful event is at position 3
    last_event = final_events[-1]
    assert last_event["stream_position"] == 3
    print(f"   ✓ New event at position 3")
    
    print("\n" + "="*60)
    print("✓ TEST PASSED: Double-decision concurrency works correctly")
    print("="*60)


@pytest.mark.asyncio
async def test_concurrency_with_retry(event_store):
    """
    Test that losing agent can retry successfully after reloading.
    
    Scenario:
    1. Two agents collide (one wins, one loses)
    2. Losing agent reloads stream, sees new event
    3. Losing agent retries with updated version
    4. Both operations succeed (one on first try, one on retry)
    """
    print("\n" + "="*60)
    print("TEST: Concurrency with Retry")
    print("="*60)
    
    stream_id = f"loan-retry-test-{uuid4()}"
    
    # Create initial event
    event1 = ApplicationSubmitted(
        event_type="ApplicationSubmitted",
        event_version=1,
        event_id=uuid4(),
        recorded_at=datetime.utcnow(),
        application_id="retry-test",
        applicant_id="CUST-001",
        requested_amount_usd=Decimal("500000"),
        loan_purpose="expansion",
        loan_term_months=60,
        submission_channel="web",
        contact_email="test@example.com",
        contact_name="Test User",
        application_reference="REF-001"
    )
    
    await event_store.append(
        stream_id=stream_id,
        events=[event1.to_store_dict()],
        expected_version=-1
    )
    
    print(f"\n1. Initial stream created with version 0")
    
    async def winning_agent():
        """Agent that will win the first race"""
        # Read current version
        version = await event_store.stream_version(stream_id)
        print(f"   Winning agent read version {version}")
        
        await asyncio.sleep(0.02)  # Simulate processing
        
        # Create credit decision
        decision = CreditDecision(
            risk_tier=RiskTier.LOW,
            recommended_limit_usd=Decimal("450000"),
            confidence=0.95,
            rationale="Strong financials",
            key_concerns=[],
            data_quality_caveats=[],
            policy_overrides_applied=[]
        )
        
        event = CreditAnalysisCompleted(
            event_type="CreditAnalysisCompleted",
            event_version=2,
            event_id=uuid4(),
            recorded_at=datetime.utcnow(),
            application_id="retry-test",
            session_id="session-win",
            decision=decision,
            model_version="credit-model-v2",
            model_deployment_id="deploy-123",
            input_data_hash="hash123",
            analysis_duration_ms=150,
            regulatory_basis=[],
            completed_at=datetime.utcnow()
        )
        
        positions = await event_store.append(
            stream_id=stream_id,
            events=[event.to_store_dict()],
            expected_version=version
        )
        
        print(f"   Winning agent appended at position {positions[0]}")
        return {"agent": "win", "success": True, "position": positions[0]}
    
    async def losing_and_retry_agent():
        """Agent that loses first attempt but retries successfully"""
        # First attempt - will lose
        version = await event_store.stream_version(stream_id)
        print(f"   Losing agent read version {version}")
        
        await asyncio.sleep(0.01)  # Slightly faster to ensure collision
        
        decision = CreditDecision(
            risk_tier=RiskTier.MEDIUM,
            recommended_limit_usd=Decimal("400000"),
            confidence=0.85,
            rationale="Average financials",
            key_concerns=[],
            data_quality_caveats=[],
            policy_overrides_applied=[]
        )
        
        event = CreditAnalysisCompleted(
            event_type="CreditAnalysisCompleted",
            event_version=2,
            event_id=uuid4(),
            recorded_at=datetime.utcnow(),
            application_id="retry-test",
            session_id="session-lose",
            decision=decision,
            model_version="credit-model-v2",
            model_deployment_id="deploy-123",
            input_data_hash="hash456",
            analysis_duration_ms=150,
            regulatory_basis=[],
            completed_at=datetime.utcnow()
        )
        
        try:
            # First attempt - will fail due to concurrency
            await event_store.append(
                stream_id=stream_id,
                events=[event.to_store_dict()],
                expected_version=version
            )
            # Should not reach here
            return {"agent": "lose", "success": True}
            
        except OptimisticConcurrencyError as e:
            print(f"   Losing agent caught OCC error: {e}")
            
            # RELOAD: Read the stream to see what happened
            new_version = await event_store.stream_version(stream_id)
            events = await event_store.load_stream(stream_id)
            print(f"   Losing agent reloaded: now version {new_version}, {len(events)} events")
            
            # Verify winning agent's event is there
            assert len(events) == 2  # Initial + winning agent's event
            
            # RETRY: Now try again with updated version
            decision2 = CreditDecision(
                risk_tier=RiskTier.MEDIUM,
                recommended_limit_usd=Decimal("400000"),
                confidence=0.85,
                rationale="Average financials (retry)",
                key_concerns=[],
                data_quality_caveats=[],
                policy_overrides_applied=[]
            )
            
            event2 = CreditAnalysisCompleted(
                event_type="CreditAnalysisCompleted",
                event_version=2,
                event_id=uuid4(),
                recorded_at=datetime.utcnow(),
                application_id="retry-test",
                session_id="session-lose-retry",
                decision=decision2,
                model_version="credit-model-v2",
                model_deployment_id="deploy-123",
                input_data_hash="hash789",
                analysis_duration_ms=150,
                regulatory_basis=[],
                completed_at=datetime.utcnow()
            )
            
            # Retry with new version
            positions = await event_store.append(
                stream_id=stream_id,
                events=[event2.to_store_dict()],
                expected_version=new_version
            )
            
            print(f"   Losing agent retry succeeded at position {positions[0]}")
            return {"agent": "lose", "success": True, "retry": True, "position": positions[0]}
    
    # Run both agents
    print("\n2. Running agents with retry logic...")
    results = await asyncio.gather(
        winning_agent(),
        losing_and_retry_agent()
    )
    
    # Verify both succeeded (one on first try, one on retry)
    assert all(r["success"] for r in results)
    
    # Verify stream now has 3 events (initial + 2 analyses)
    final_events = await event_store.load_stream(stream_id)
    assert len(final_events) == 3, f"Expected 3 events, got {len(final_events)}"
    
    print("\n3. Final result:")
    print(f"   ✓ Both agents succeeded (one on first try, one on retry)")
    print(f"   ✓ Final stream has {len(final_events)} events")
    print("\n✓ TEST PASSED: Retry flow works correctly")


@pytest.mark.asyncio
async def test_high_contention_simulation(event_store):
    """
    Test with many concurrent writers to simulate high-load scenarios.
    
    Launches 10 concurrent agents all trying to append to the same stream.
    Verifies that exactly one succeeds and others fail with OCC errors.
    """
    print("\n" + "="*60)
    print("TEST: High Contention Simulation (10 concurrent writers)")
    print("="*60)
    
    stream_id = f"loan-contention-{uuid4()}"
    
    # Create initial event
    event1 = ApplicationSubmitted(
        event_type="ApplicationSubmitted",
        event_version=1,
        event_id=uuid4(),
        recorded_at=datetime.utcnow(),
        application_id="contention-test",
        applicant_id="CUST-001",
        requested_amount_usd=Decimal("500000"),
        loan_purpose="expansion",
        loan_term_months=60,
        submission_channel="web",
        contact_email="test@example.com",
        contact_name="Test User",
        application_reference="REF-001"
    )
    
    await event_store.append(
        stream_id=stream_id,
        events=[event1.to_store_dict()],
        expected_version=-1
    )
    
    current_version = await event_store.stream_version(stream_id)
    print(f"\n1. Initial stream created with version {current_version}")
    
    async def contention_agent(agent_id: int):
        """Agent that tries to append"""
        try:
            # Read current version
            version = await event_store.stream_version(stream_id)
            
            # Small random delay to simulate real-world timing
            await asyncio.sleep(0.005 * (agent_id % 5))
            
            decision = CreditDecision(
                risk_tier=RiskTier.MEDIUM,
                recommended_limit_usd=Decimal("400000"),
                confidence=0.85,
                rationale=f"Analysis by agent {agent_id}",
                key_concerns=[],
                data_quality_caveats=[],
                policy_overrides_applied=[]
            )
            
            event = CreditAnalysisCompleted(
                event_type="CreditAnalysisCompleted",
                event_version=2,
                event_id=uuid4(),
                recorded_at=datetime.utcnow(),
                application_id="contention-test",
                session_id=f"session-{agent_id}",
                decision=decision,
                model_version="credit-model-v2",
                model_deployment_id="deploy-123",
                input_data_hash=f"hash{agent_id}",
                analysis_duration_ms=100,
                regulatory_basis=[],
                completed_at=datetime.utcnow()
            )
            
            positions = await event_store.append(
                stream_id=stream_id,
                events=[event.to_store_dict()],
                expected_version=version
            )
            
            return {"agent": agent_id, "success": True, "position": positions[0]}
            
        except OptimisticConcurrencyError as e:
            return {"agent": agent_id, "success": False, "error": str(e)}
    
    # Launch 10 concurrent agents
    print("\n2. Launching 10 concurrent agents...")
    tasks = [contention_agent(i) for i in range(10)]
    results = await asyncio.gather(*tasks)
    
    # Analyze results
    successes = [r for r in results if r["success"]]
    failures = [r for r in results if not r["success"]]
    
    print(f"\n3. Results:")
    print(f"   Successful agents: {len(successes)}")
    print(f"   Failed agents: {len(failures)}")
    
    # Exactly one should succeed
    assert len(successes) == 1, f"Expected 1 success, got {len(successes)}"
    
    # All others should fail
    assert len(failures) == 9, f"Expected 9 failures, got {len(failures)}"
    
    # Final stream should have 2 events (initial + 1 success)
    final_events = await event_store.load_stream(stream_id)
    assert len(final_events) == 2, f"Expected 2 events, got {len(final_events)}"
    
    print(f"\n4. Final stream has {len(final_events)} events")
    print(f"   Winning agent: {successes[0]['agent']} at position {successes[0]['position']}")
    
    print("\n✓ TEST PASSED: High contention handled correctly")


@pytest.mark.asyncio
async def test_causal_consistency(event_store):
    """
    Test that causal relationships are preserved across concurrent writes.
    
    Scenario:
    1. Agent A writes event X
    2. Agent B reads X and writes event Y (causally dependent on X)
    3. Even with concurrency, Y must appear after X in the stream
    """
    print("\n" + "="*60)
    print("TEST: Causal Consistency")
    print("="*60)
    
    stream_id = f"loan-causal-{uuid4()}"
    
    # Create initial event
    event1 = ApplicationSubmitted(
        event_type="ApplicationSubmitted",
        event_version=1,
        event_id=uuid4(),
        recorded_at=datetime.utcnow(),
        application_id="causal-test",
        applicant_id="CUST-001",
        requested_amount_usd=Decimal("500000"),
        loan_purpose="expansion",
        loan_term_months=60,
        submission_channel="web",
        contact_email="test@example.com",
        contact_name="Test User",
        application_reference="REF-001"
    )
    
    positions = await event_store.append(
        stream_id=stream_id,
        events=[event1.to_store_dict()],
        expected_version=-1
    )
    
    print(f"\n1. Initial event at position {positions[0]}")
    
    # Store the event ID for causation
    cause_event_id = str(event1.event_id)
    
    async def agent_a():
        """Agent A writes an event"""
        decision = CreditDecision(
            risk_tier=RiskTier.MEDIUM,
            recommended_limit_usd=Decimal("400000"),
            confidence=0.85,
            rationale="Analysis by Agent A",
            key_concerns=[],
            data_quality_caveats=[],
            policy_overrides_applied=[]
        )
        
        event = CreditAnalysisCompleted(
            event_type="CreditAnalysisCompleted",
            event_version=2,
            event_id=uuid4(),
            recorded_at=datetime.utcnow(),
            application_id="causal-test",
            session_id="session-a",
            decision=decision,
            model_version="credit-model-v2",
            model_deployment_id="deploy-123",
            input_data_hash="hash-a",
            analysis_duration_ms=100,
            regulatory_basis=[],
            completed_at=datetime.utcnow()
        )
        
        positions = await event_store.append(
            stream_id=stream_id,
            events=[event.to_store_dict()],
            expected_version=0,
            causation_id=cause_event_id  # Caused by initial event
        )
        
        print(f"   Agent A wrote event at position {positions[0]}")
        return {"event_id": str(event.event_id), "position": positions[0]}
    
    async def agent_b(a_event_id):
        """Agent B reads A's event and writes a dependent event"""
        # Wait for A to complete
        await asyncio.sleep(0.05)
        
        # Read the stream to get current version
        version = await event_store.stream_version(stream_id)
        
        decision = CreditDecision(
            risk_tier=RiskTier.HIGH,
            recommended_limit_usd=Decimal("300000"),
            confidence=0.75,
            rationale="Analysis by Agent B (dependent on A)",
            key_concerns=["Based on A's analysis"],
            data_quality_caveats=[],
            policy_overrides_applied=[]
        )
        
        event = CreditAnalysisCompleted(
            event_type="CreditAnalysisCompleted",
            event_version=2,
            event_id=uuid4(),
            recorded_at=datetime.utcnow(),
            application_id="causal-test",
            session_id="session-b",
            decision=decision,
            model_version="credit-model-v2",
            model_deployment_id="deploy-123",
            input_data_hash="hash-b",
            analysis_duration_ms=100,
            regulatory_basis=[],
            completed_at=datetime.utcnow()
        )
        
        positions = await event_store.append(
            stream_id=stream_id,
            events=[event.to_store_dict()],
            expected_version=version,
            causation_id=a_event_id  # Caused by Agent A's event
        )
        
        print(f"   Agent B wrote event at position {positions[0]}")
        return positions[0]
    
    # Run agents
    print("\n2. Running causally dependent agents...")
    a_result = await agent_a()
    b_position = await agent_b(a_result["event_id"])
    
    # Verify ordering
    assert b_position > a_result["position"], \
        f"Causal violation: B's event ({b_position}) should be after A's ({a_result['position']})"
    
    print(f"\n3. Causal ordering preserved: {a_result['position']} → {b_position}")
    print("\n✓ TEST PASSED: Causal consistency maintained")


@pytest.mark.asyncio
async def test_concurrency_edge_cases(event_store):
    """
    Test edge cases for optimistic concurrency:
    
    1. Appending to non-existent stream with wrong expected_version
    2. Appending to existing stream with wrong expected_version
    3. Empty events list
    """
    print("\n" + "="*60)
    print("TEST: Concurrency Edge Cases")
    print("="*60)
    
    stream_id = f"loan-edge-{uuid4()}"
    
    # 1. Appending to non-existent stream with wrong expected_version
    print("\n1. Testing append to non-existent stream...")
    
    decision = CreditDecision(
        risk_tier=RiskTier.MEDIUM,
        recommended_limit_usd=Decimal("400000"),
        confidence=0.85,
        rationale="Test",
        key_concerns=[],
        data_quality_caveats=[],
        policy_overrides_applied=[]
    )
    
    event = CreditAnalysisCompleted(
        event_type="CreditAnalysisCompleted",
        event_version=2,
        event_id=uuid4(),
        recorded_at=datetime.utcnow(),
        application_id="edge-test",
        session_id="session-edge",
        decision=decision,
        model_version="credit-model-v2",
        model_deployment_id="deploy-123",
        input_data_hash="hash-edge",
        analysis_duration_ms=100,
        regulatory_basis=[],
        completed_at=datetime.utcnow()
    )
    
    # Try with expected_version=0 (stream doesn't exist, should be -1)
    with pytest.raises(OptimisticConcurrencyError) as excinfo:
        await event_store.append(
            stream_id=stream_id,
            events=[event.to_store_dict()],
            expected_version=0  # Wrong - should be -1 for new stream
        )
    print(f"   ✓ Correctly rejected: {excinfo.value}")
    
    # 2. Create stream properly
    app_event = ApplicationSubmitted(
        event_type="ApplicationSubmitted",
        event_version=1,
        event_id=uuid4(),
        recorded_at=datetime.utcnow(),
        application_id="edge-test",
        applicant_id="CUST-001",
        requested_amount_usd=Decimal("500000"),
        loan_purpose="expansion",
        loan_term_months=60,
        submission_channel="web",
        contact_email="test@example.com",
        contact_name="Test User",
        application_reference="REF-001"
    )
    
    await event_store.append(
        stream_id=stream_id,
        events=[app_event.to_store_dict()],
        expected_version=-1  # Correct for new stream
    )
    print(f"\n2. Stream created with version 0")
    
    # 3. Try to append with wrong version
    print("\n3. Testing append with wrong expected_version...")
    
    with pytest.raises(OptimisticConcurrencyError) as excinfo:
        await event_store.append(
            stream_id=stream_id,
            events=[event.to_store_dict()],
            expected_version=5  # Way off
        )
    print(f"   ✓ Correctly rejected: {excinfo.value}")
    
    # 4. Try to append empty events list
    print("\n4. Testing append with empty events list...")
    
    with pytest.raises(ValueError, match="No events to append"):
        await event_store.append(
            stream_id=stream_id,
            events=[],
            expected_version=0
        )
    print(f"   ✓ Correctly rejected empty events")
    
    print("\n✓ TEST PASSED: All edge cases handled correctly")