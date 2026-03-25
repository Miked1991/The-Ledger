# tests/test_projection_daemon.py
"""
Comprehensive tests for projection daemon and async projections.
"""

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
async def projections(db_pool: asyncpg.Pool, event_store: EventStore):
    """Create projection instances"""
    app_summary = ApplicationSummaryProjection()
    agent_perf = AgentPerformanceProjection()
    compliance = ComplianceAuditProjection()
    
    app_summary.set_pool(db_pool)
    agent_perf.set_pool(db_pool)
    compliance.set_pool(db_pool)
    
    app_summary.set_store(event_store)
    agent_perf.set_store(event_store)
    compliance.set_store(event_store)
    
    return {
        "app_summary": app_summary,
        "agent_perf": agent_perf,
        "compliance": compliance
    }


@pytest.mark.asyncio
async def test_projection_daemon_initialization(event_store: EventStore, projections, db_pool):
    """Test projection daemon initialization"""
    daemon = ProjectionDaemon(
        event_store,
        [projections["app_summary"], projections["agent_perf"], projections["compliance"]],
        max_retries=3,
        batch_size=100,
        poll_interval_ms=100
    )
    
    # Set pools
    for proj in daemon._projections.values():
        proj.set_pool(db_pool)
    
    assert daemon._running is False
    assert len(daemon._async_projections) == 2  # agent_perf and compliance are async
    assert len(daemon._projections) == 3
    
    # Load checkpoints (should be 0 initially)
    await daemon._load_checkpoints()
    for name in daemon._async_projections:
        assert daemon._checkpoints[name] == 0


@pytest.mark.asyncio
async def test_projection_daemon_batch_processing(event_store: EventStore, projections, db_pool):
    """Test that projection daemon processes events in batches"""
    daemon = ProjectionDaemon(
        event_store,
        [projections["app_summary"], projections["agent_perf"], projections["compliance"]],
        max_retries=3,
        batch_size=50,
        poll_interval_ms=10
    )
    
    for proj in daemon._projections.values():
        proj.set_pool(db_pool)
    
    await daemon._load_checkpoints()
    
    # Create multiple events
    for i in range(25):
        app_id = f"batch-test-{i}"
        await event_store.append(
            f"loan-{app_id}",
            [ApplicationSubmitted(
                application_id=app_id,
                applicant_id=f"applicant-{i}",
                requested_amount_usd=100000,
                business_name=f"Business {i}",
                tax_id=f"12-{i}345678"
            )],
            expected_version=-1
        )
    
    # Process batch
    await daemon._process_batch()
    
    # Check that checkpoints were updated
    for name in daemon._async_projections:
        assert daemon._checkpoints[name] > 0
    
    # Verify stats
    stats = daemon.get_stats()
    assert stats["total_processed"] >= 25
    assert stats["running"] is False


@pytest.mark.asyncio
async def test_projection_daemon_fault_tolerance(event_store: EventStore, db_pool):
    """Test that projection daemon handles errors gracefully"""
    
    # Create a faulty projection that fails on specific events
    class FaultyProjection(ComplianceAuditProjection):
        def __init__(self):
            super().__init__()
            self.name = "faulty"
            self.fail_count = 0
            self.fail_on_event = 2
        
        async def handle_batch(self, events):
            for event in events:
                self.fail_count += 1
                if self.fail_count == self.fail_on_event:
                    raise Exception("Simulated projection failure")
                await super().handle_batch([event])
    
    faulty = FaultyProjection()
    faulty.set_pool(db_pool)
    faulty.set_store(event_store)
    
    daemon = ProjectionDaemon(event_store, [faulty], max_retries=2, batch_size=10)
    faulty.set_pool(db_pool)
    
    await daemon._load_checkpoints()
    
    # Create events
    for i in range(5):
        await event_store.append(
            f"loan-faulty-{i}",
            [ApplicationSubmitted(
                application_id=f"faulty-app-{i}",
                applicant_id=f"applicant-{i}",
                requested_amount_usd=100000,
                business_name=f"Business {i}",
                tax_id=f"12-{i}345678"
            )],
            expected_version=-1
        )
    
    # Process batch
    await daemon._process_batch()
    
    # Verify that the faulty event was skipped after retries
    assert faulty.fail_count >= 2
    
    # Check dead letter queue
    dead_letter = await daemon.get_dead_letter_queue()
    assert len(dead_letter) > 0
    assert dead_letter[0]["projection"] == "faulty"


@pytest.mark.asyncio
async def test_projection_rebuild(event_store: EventStore, projections, db_pool):
    """Test rebuilding projections from scratch"""
    app_summary = projections["app_summary"]
    app_summary.set_pool(db_pool)
    app_summary.set_store(event_store)
    
    # Create events
    for i in range(10):
        app_id = f"rebuild-app-{i}"
        await event_store.append(
            f"loan-{app_id}",
            [ApplicationSubmitted(
                application_id=app_id,
                applicant_id=f"applicant-{i}",
                requested_amount_usd=100000,
                business_name=f"Business {i}",
                tax_id=f"12-{i}345678"
            )],
            expected_version=-1
        )
        
        # Update projection directly
        loaded = await event_store.load_stream(f"loan-{app_id}")
        await app_summary.handle_event(loaded[0])
    
    # Verify projections exist
    for i in range(10):
        app_data = await app_summary.get_application(f"rebuild-app-{i}")
        assert app_data is not None
        assert app_data["application_id"] == f"rebuild-app-{i}"
    
    # Rebuild from scratch
    await app_summary.rebuild(event_store)
    
    # Verify all data still present
    for i in range(10):
        app_data = await app_summary.get_application(f"rebuild-app-{i}")
        assert app_data is not None
        assert app_data["application_id"] == f"rebuild-app-{i}"


@pytest.mark.asyncio
async def test_projection_lag_metrics(event_store: EventStore, projections, db_pool):
    """Test that projection lag metrics are calculated correctly"""
    daemon = ProjectionDaemon(
        event_store,
        [projections["app_summary"], projections["agent_perf"], projections["compliance"]],
        batch_size=10
    )
    
    for proj in daemon._projections.values():
        proj.set_pool(db_pool)
    
    await daemon._load_checkpoints()
    
    # Create events
    for i in range(20):
        await event_store.append(
            f"loan-lag-{i}",
            [ApplicationSubmitted(
                application_id=f"lag-app-{i}",
                applicant_id=f"applicant-{i}",
                requested_amount_usd=100000,
                business_name=f"Business {i}",
                tax_id=f"12-{i}345678"
            )],
            expected_version=-1
        )
    
    # Process batch
    await daemon._process_batch()
    
    # Check lag metrics
    lags = await daemon.get_all_lags()
    assert "application_summary" in lags or "agent_performance" in lags
    
    for name, lag in lags.items():
        assert "events" in lag
        assert "estimated_ms" in lag


@pytest.mark.asyncio
async def test_dead_letter_queue_processing(event_store: EventStore, db_pool):
    """Test dead letter queue processing"""
    
    class FailingProjection(AgentPerformanceProjection):
        def __init__(self):
            super().__init__()
            self.name = "failing"
            self.should_fail = True
        
        async def handle_batch(self, events):
            if self.should_fail:
                raise Exception("Simulated persistent failure")
            await super().handle_batch(events)
    
    failing = FailingProjection()
    failing.set_pool(db_pool)
    failing.set_store(event_store)
    
    daemon = ProjectionDaemon(event_store, [failing], max_retries=1, dead_letter_queue_enabled=True)
    failing.set_pool(db_pool)
    
    await daemon._load_checkpoints()
    
    # Create events
    await event_store.append(
        "loan-deadletter",
        [ApplicationSubmitted(
            application_id="deadletter-app",
            applicant_id="test",
            requested_amount_usd=100000,
            business_name="Test Corp",
            tax_id="12-3456789"
        )],
        expected_version=-1
    )
    
    # Process batch (should fail and go to dead letter)
    await daemon._process_batch()
    
    # Check dead letter queue
    dead_letter = await daemon.get_dead_letter_queue()
    assert len(dead_letter) > 0
    
    # Fix the projection
    failing.should_fail = False
    
    # Reprocess dead letter
    await daemon.reprocess_dead_letter()
    
    # Queue should be empty
    dead_letter = await daemon.get_dead_letter_queue()
    assert len(dead_letter) == 0