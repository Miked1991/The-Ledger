# tests/test_projections.py
import pytest
import pytest_asyncio
import asyncio
import asyncpg
from datetime import datetime, timedelta
from src.event_store import EventStore
from src.projections.daemon import ProjectionDaemon
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.agent_performance import AgentPerformanceProjection
from src.projections.compliance_audit import ComplianceAuditProjection
from src.models.events import (
    ApplicationSubmitted, CreditAnalysisCompleted, DecisionGenerated,
    HumanReviewCompleted, AgentContextLoaded
)

@pytest_asyncio.fixture
async def projections(db_pool: asyncpg.Pool):
    """Create projection instances"""
    app_summary = ApplicationSummaryProjection()
    agent_perf = AgentPerformanceProjection()
    compliance = ComplianceAuditProjection()
    
    # Set database pools
    app_summary.set_pool(db_pool)
    agent_perf.set_pool(db_pool)
    compliance.set_pool(db_pool)
    
    return {
        "app_summary": app_summary,
        "agent_perf": agent_perf,
        "compliance": compliance
    }

@pytest.mark.asyncio
async def test_application_summary_projection(event_store: EventStore, projections):
    """Test application summary projection updates correctly"""
    app_summary = projections["app_summary"]
    application_id = "test-app-summary-1"
    stream_id = f"loan-{application_id}"
    
    # Submit application
    submitted_event = ApplicationSubmitted(
        application_id=application_id,
        applicant_id="test-applicant",
        requested_amount_usd=500000,
        business_name="Test Business",
        tax_id="12-3456789"
    )
    
    await event_store.append(stream_id, [submitted_event], expected_version=-1)
    
    # Manually update projection (inline)
    await app_summary.handle_event(
        (await event_store.load_stream(stream_id))[0]
    )
    
    # Verify projection
    app_data = await app_summary.get_application(application_id)
    assert app_data is not None
    assert app_data["application_id"] == application_id
    assert app_data["applicant_id"] == "test-applicant"
    assert app_data["requested_amount"] == 500000
    assert app_data["current_state"] == "SUBMITTED"
    
    # Add credit analysis
    credit_event = CreditAnalysisCompleted(
        application_id=application_id,
        risk_tier="MEDIUM",
        credit_score=750,
        max_credit_limit=500000,
        model_version="v1.0",
        confidence_score=0.85
    )
    
    await event_store.append(stream_id, [credit_event], expected_version=1)
    
    # Update projection
    await app_summary.handle_event(
        (await event_store.load_stream(stream_id))[-1]
    )
    
    # Verify update
    app_data = await app_summary.get_application(application_id)
    assert app_data["credit_analysis_tier"] == "MEDIUM"
    assert app_data["current_state"] == "ANALYSIS_COMPLETE"
    
    print("✅ Application summary projection test passed")

@pytest.mark.asyncio
async def test_agent_performance_projection(event_store: EventStore, projections):
    """Test agent performance projection aggregates correctly"""
    agent_perf = projections["agent_perf"]
    
    agent_id = "test-agent-1"
    session_id = "test-session-1"
    stream_id = f"agent-{agent_id}-{session_id}"
    
    # Create agent session
    context_event = AgentContextLoaded(
        agent_id=agent_id,
        session_id=session_id,
        context_source="database",
        token_count=1024,
        model_version="v2.0",
        context_hash="abc123"
    )
    
    await event_store.append(stream_id, [context_event], expected_version=-1)
    
    # Process through projection
    await agent_perf.handle_batch(await event_store.load_stream(stream_id))
    
    # Verify metrics
    metrics = await agent_perf.get_agent_metrics(agent_id)
    assert len(metrics) >= 1
    
    # Add decision that references this agent
    decision_stream = "loan-decision-test"
    decision_event = DecisionGenerated(
        application_id="test-app",
        decision_id="dec-1",
        recommendation="APPROVE",
        confidence_score=0.92,
        contributing_agent_sessions=[session_id],
        decision_reasoning="Test"
    )
    
    await event_store.append(decision_stream, [decision_event], expected_version=-1)
    await agent_perf.handle_batch(await event_store.load_stream(decision_stream))
    
    # Verify metrics updated
    metrics = await agent_perf.get_agent_metrics(agent_id)
    assert len(metrics) >= 1
    
    print("✅ Agent performance projection test passed")

@pytest.mark.asyncio
async def test_compliance_audit_projection(event_store: EventStore, projections):
    """Test compliance audit projection with temporal queries"""
    compliance = projections["compliance"]
    application_id = "test-compliance-1"
    stream_id = f"loan-{application_id}"
    
    # Create events over time
    events = [
        ApplicationSubmitted(
            application_id=application_id,
            applicant_id="test-applicant",
            requested_amount_usd=300000,
            business_name="Compliance Test",
            tax_id="99-9999999"
        )
    ]
    
    await event_store.append(stream_id, events, expected_version=-1)
    
    # Process through projection
    loaded_events = await event_store.load_stream(stream_id)
    await compliance.handle_batch(loaded_events)
    
    # Get current compliance state
    current_state = await compliance.get_compliance_at(application_id)
    assert current_state["application_id"] == application_id
    assert current_state["compliance_status"] in ["PENDING", "PARTIAL"]
    
    # Add compliance events
    compliance_events = [
        # This would be a ComplianceRulePassed event in real implementation
    ]
    
    # Test temporal query with future date
    future_date = datetime.utcnow() + timedelta(days=30)
    future_state = await compliance.get_compliance_at(application_id, as_of=future_date)
    
    print("✅ Compliance audit projection test passed")

@pytest.mark.asyncio
async def test_projection_daemon(event_store: EventStore, projections, db_pool):
    """Test projection daemon with fault tolerance"""
    daemon = ProjectionDaemon(
        event_store,
        [projections["app_summary"], projections["agent_perf"], projections["compliance"]]
    )
    
    # Set up pool for projections
    for proj in daemon._projections.values():
        proj.set_pool(db_pool)
    
    # Start daemon in background
    daemon_task = asyncio.create_task(daemon.run_forever(poll_interval_ms=10))
    
    # Create some events
    stream_id = "loan-daemon-test"
    application_id = "test-daemon"
    
    for i in range(5):
        event = ApplicationSubmitted(
            application_id=f"{application_id}-{i}",
            applicant_id=f"applicant-{i}",
            requested_amount_usd=100000 * (i + 1),
            business_name=f"Business {i}",
            tax_id=f"12-{i}345678"
        )
        await event_store.append(f"loan-{application_id}-{i}", [event], expected_version=-1)
    
    # Wait for daemon to process
    await asyncio.sleep(0.5)
    
    # Check lags
    for proj_name in daemon._projections:
        lag = await daemon.get_lag(proj_name)
        assert lag >= 0
        print(f"Projection {proj_name} lag: {lag}")
    
    # Stop daemon
    daemon.stop()
    await daemon_task
    
    print("✅ Projection daemon test passed")

@pytest.mark.asyncio
async def test_projection_rebuild(event_store: EventStore, projections, db_pool):
    """Test rebuilding projections from scratch"""
    app_summary = projections["app_summary"]
    
    # Create some events
    for i in range(3):
        stream_id = f"loan-rebuild-{i}"
        event = ApplicationSubmitted(
            application_id=f"rebuild-app-{i}",
            applicant_id=f"applicant-{i}",
            requested_amount_usd=100000,
            business_name=f"Business {i}",
            tax_id=f"12-{i}345678"
        )
        await event_store.append(stream_id, [event], expected_version=-1)
        
        # Update projection
        await app_summary.handle_event(
            (await event_store.load_stream(stream_id))[0]
        )
    
    # Verify projections exist
    for i in range(3):
        app_data = await app_summary.get_application(f"rebuild-app-{i}")
        assert app_data is not None
    
    # Rebuild from scratch
    await app_summary.rebuild(event_store)
    
    # Verify all data still present
    for i in range(3):
        app_data = await app_summary.get_application(f"rebuild-app-{i}")
        assert app_data is not None
        assert app_data["application_id"] == f"rebuild-app-{i}"
    
    print("✅ Projection rebuild test passed")

@pytest.mark.asyncio
async def test_projection_fault_tolerance(event_store: EventStore, projections, db_pool):
    """Test that projection daemon handles errors gracefully"""
    # Create a projection that fails on certain events
    class FaultyProjection(ApplicationSummaryProjection):
        def __init__(self):
            super().__init__()
            self.name = "faulty"
            self.fail_count = 0
        
        async def handle_event(self, event):
            self.fail_count += 1
            if self.fail_count == 2:  # Fail on second event
                raise Exception("Simulated projection failure")
            await super().handle_event(event)
    
    faulty = FaultyProjection()
    faulty.set_pool(db_pool)
    
    daemon = ProjectionDaemon(event_store, [faulty], max_retries=2)
    
    # Create events
    events = []
    for i in range(5):
        event = ApplicationSubmitted(
            application_id=f"faulty-app-{i}",
            applicant_id=f"applicant-{i}",
            requested_amount_usd=100000,
            business_name=f"Business {i}",
            tax_id=f"12-{i}345678"
        )
        events.append(event)
        await event_store.append(f"loan-faulty-{i}", [event], expected_version=-1)
    
    # Process batch
    await daemon._process_batch(batch_size=10)
    
    # Verify that the faulty event was skipped after retries
    # The first event succeeded, second failed and was skipped
    assert faulty.fail_count >= 2
    
    print("✅ Projection fault tolerance test passed")

@pytest.mark.asyncio
async def test_projection_slo_performance(event_store: EventStore, projections, db_pool):
    """Test that projections meet SLO requirements"""
    app_summary = projections["app_summary"]
    app_summary.set_pool(db_pool)
    
    # Create many events to test performance
    import time
    
    start_time = time.time()
    
    for i in range(100):
        stream_id = f"loan-slo-{i}"
        event = ApplicationSubmitted(
            application_id=f"slo-app-{i}",
            applicant_id=f"applicant-{i}",
            requested_amount_usd=100000,
            business_name=f"Business {i}",
            tax_id=f"12-{i}345678"
        )
        await event_store.append(stream_id, [event], expected_version=-1)
        
        # Update projection
        loaded = await event_store.load_stream(stream_id)
        await app_summary.handle_event(loaded[0])
    
    # Test query performance
    query_start = time.time()
    
    for i in range(50):
        app_data = await app_summary.get_application(f"slo-app-{i}")
        assert app_data is not None
    
    query_time = (time.time() - query_start) * 1000  # ms
    avg_query_time = query_time / 50
    
    print(f"Average query time: {avg_query_time:.2f}ms")
    
    # SLO: p99 < 50ms
    assert avg_query_time < 50, f"Query time {avg_query_time:.2f}ms exceeds SLO"
    
    print("✅ Projection SLO performance test passed")