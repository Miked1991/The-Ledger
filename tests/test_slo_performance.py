# tests/test_slo_performance.py
"""
SLO-focused performance tests with quantitative measurements.
Ensures all system components meet their Service Level Objectives.
"""

import pytest
import pytest_asyncio
import asyncio
import time
import statistics
from typing import List, Dict, Any
from datetime import datetime, timedelta
from src.event_store import EventStore
from src.commands.handlers import CommandHandlers
from src.projections.daemon import ProjectionDaemon
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.agent_performance import AgentPerformanceProjection
from src.projections.compliance_audit import ComplianceAuditProjection
from src.models.events import ApplicationSubmitted, CreditAnalysisCompleted


class SLOMetrics:
    """Collect and analyze SLO metrics"""
    
    def __init__(self):
        self.latencies: List[float] = []
        self.start_time: float = 0
        self.end_time: float = 0
    
    def start(self):
        self.start_time = time.time()
    
    def stop(self) -> float:
        self.end_time = time.time()
        return self.end_time - self.start_time
    
    def record_latency(self, latency_ms: float):
        self.latencies.append(latency_ms)
    
    def get_stats(self) -> Dict[str, float]:
        if not self.latencies:
            return {"count": 0}
        
        sorted_latencies = sorted(self.latencies)
        p50 = sorted_latencies[int(len(sorted_latencies) * 0.5)]
        p90 = sorted_latencies[int(len(sorted_latencies) * 0.9)]
        p95 = sorted_latencies[int(len(sorted_latencies) * 0.95)]
        p99 = sorted_latencies[int(len(sorted_latencies) * 0.99)]
        
        return {
            "count": len(self.latencies),
            "min": min(self.latencies),
            "max": max(self.latencies),
            "mean": statistics.mean(self.latencies),
            "p50": p50,
            "p90": p90,
            "p95": p95,
            "p99": p99
        }
    
    def assert_slo(self, slo_ms: float, percentile: float = 0.99) -> bool:
        """Assert that the SLO is met"""
        stats = self.get_stats()
        if percentile == 0.99:
            actual = stats.get("p99", float("inf"))
        elif percentile == 0.95:
            actual = stats.get("p95", float("inf"))
        else:
            actual = stats.get("mean", float("inf"))
        
        return actual <= slo_ms


@pytest.mark.asyncio
async def test_event_append_slo(event_store: EventStore):
    """Test that event append operations meet SLO (<100ms p99)"""
    metrics = SLOMetrics()
    
    # Run 1000 append operations
    for i in range(1000):
        start = time.time()
        
        await event_store.append(
            f"slo-test-{i % 100}",
            [ApplicationSubmitted(
                application_id=f"slo-app-{i}",
                applicant_id=f"applicant-{i}",
                requested_amount_usd=100000,
                business_name=f"Business {i}",
                tax_id=f"12-{i}345678"
            )],
            expected_version=-1
        )
        
        latency_ms = (time.time() - start) * 1000
        metrics.record_latency(latency_ms)
    
    stats = metrics.get_stats()
    
    # Assert SLO: p99 < 100ms
    assert metrics.assert_slo(100, 0.99), f"p99 latency {stats['p99']:.2f}ms exceeds 100ms SLO"
    print(f"\n📊 Event Append SLO Results:")
    print(f"   p50: {stats['p50']:.2f}ms")
    print(f"   p95: {stats['p95']:.2f}ms")
    print(f"   p99: {stats['p99']:.2f}ms")
    print(f"   Max: {stats['max']:.2f}ms")


@pytest.mark.asyncio
async def test_application_summary_query_slo(event_store: EventStore, db_pool):
    """Test that application summary queries meet SLO (<50ms p99)"""
    # Create test data
    app_ids = []
    for i in range(500):
        app_id = f"slo-summary-{i}"
        app_ids.append(app_id)
        
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
    
    # Initialize projection
    projection = ApplicationSummaryProjection()
    projection.set_pool(db_pool)
    await projection.rebuild(event_store)
    
    # Measure query latency
    metrics = SLOMetrics()
    
    for _ in range(1000):
        app_id = app_ids[_ % len(app_ids)]
        start = time.time()
        
        result = await projection.get_application(app_id)
        
        latency_ms = (time.time() - start) * 1000
        metrics.record_latency(latency_ms)
        assert result is not None
    
    stats = metrics.get_stats()
    
    # Assert SLO: p99 < 50ms
    assert metrics.assert_slo(50, 0.99), f"p99 latency {stats['p99']:.2f}ms exceeds 50ms SLO"
    print(f"\n📊 Application Summary Query SLO Results:")
    print(f"   p50: {stats['p50']:.2f}ms")
    print(f"   p95: {stats['p95']:.2f}ms")
    print(f"   p99: {stats['p99']:.2f}ms")
    print(f"   Max: {stats['max']:.2f}ms")


@pytest.mark.asyncio
async def test_compliance_audit_temporal_query_slo(event_store: EventStore, db_pool):
    """Test that compliance audit temporal queries meet SLO (<200ms p99)"""
    # Create test data with timestamps
    application_id = "slo-compliance-001"
    stream_id = f"loan-{application_id}"
    
    await event_store.append(
        stream_id,
        [ApplicationSubmitted(
            application_id=application_id,
            applicant_id="test",
            requested_amount_usd=100000,
            business_name="Test Corp",
            tax_id="12-3456789"
        )],
        expected_version=-1
    )
    
    # Add events over time
    for i in range(100):
        await asyncio.sleep(0.001)  # Small delay to create time difference
        await event_store.append(
            stream_id,
            [CreditAnalysisCompleted(
                application_id=application_id,
                risk_tier="MEDIUM",
                credit_score=750,
                max_credit_limit=100000,
                model_version="v1.0"
            )],
            expected_version=i + 1
        )
    
    # Initialize projection
    projection = ComplianceAuditProjection()
    projection.set_pool(db_pool)
    await projection.rebuild(event_store)
    
    # Measure temporal query latency
    metrics = SLOMetrics()
    
    # Query at different points in time
    for i in range(100):
        as_of = datetime.utcnow() - timedelta(seconds=i)
        start = time.time()
        
        result = await projection.get_compliance_at(application_id, as_of=as_of)
        
        latency_ms = (time.time() - start) * 1000
        metrics.record_latency(latency_ms)
    
    stats = metrics.get_stats()
    
    # Assert SLO: p99 < 200ms
    assert metrics.assert_slo(200, 0.99), f"p99 latency {stats['p99']:.2f}ms exceeds 200ms SLO"
    print(f"\n📊 Compliance Audit Temporal Query SLO Results:")
    print(f"   p50: {stats['p50']:.2f}ms")
    print(f"   p95: {stats['p95']:.2f}ms")
    print(f"   p99: {stats['p99']:.2f}ms")


@pytest.mark.asyncio
async def test_projection_lag_slo(event_store: EventStore, db_pool):
    """Test that projection lag stays within acceptable bounds"""
    # Initialize projections
    app_summary = ApplicationSummaryProjection()
    agent_perf = AgentPerformanceProjection()
    compliance = ComplianceAuditProjection()
    
    for proj in [app_summary, agent_perf, compliance]:
        proj.set_pool(db_pool)
        proj.set_store(event_store)
    
    # Start daemon
    daemon = ProjectionDaemon(
        event_store,
        [app_summary, agent_perf, compliance],
        max_retries=3,
        batch_size=100,
        poll_interval_ms=100
    )
    
    for proj in daemon._projections.values():
        proj.set_pool(db_pool)
    
    await daemon._load_checkpoints()
    
    # Write 1000 events rapidly
    start_time = time.time()
    
    for i in range(1000):
        await event_store.append(
            f"loan-lag-{i % 100}",
            [ApplicationSubmitted(
                application_id=f"lag-app-{i}",
                applicant_id=f"applicant-{i}",
                requested_amount_usd=100000,
                business_name=f"Business {i}",
                tax_id=f"12-{i}345678"
            )],
            expected_version=-1
        )
    
    write_time = (time.time() - start_time) * 1000
    
    # Process events
    await daemon._process_batch()
    
    # Check lag
    lags = await daemon.get_all_lags()
    
    # Lag should be less than 500 events
    for name, lag in lags.items():
        lag_events = lag.get("events", 0)
        assert lag_events < 500, f"Projection {name} lag {lag_events} events exceeds 500"
    
    print(f"\n📊 Projection Lag Results:")
    print(f"   Write time: {write_time:.2f}ms for 1000 events")
    for name, lag in lags.items():
        print(f"   {name}: {lag.get('events', 0)} events, {lag.get('estimated_ms', 0):.0f}ms estimated")


@pytest.mark.asyncio
async def test_concurrent_write_throughput(event_store: EventStore):
    """Test concurrent write throughput under load"""
    applications = 100
    writers_per_app = 4
    
    async def write_credit_analysis(app_id: str, agent_id: str, session_id: str):
        await event_store.append(
            f"loan-{app_id}",
            [CreditAnalysisCompleted(
                application_id=app_id,
                risk_tier="MEDIUM",
                credit_score=750,
                max_credit_limit=100000,
                model_version="v1.0"
            )],
            expected_version=0
        )
    
    # First create applications
    for i in range(applications):
        app_id = f"throughput-{i}"
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
    
    # Measure throughput
    start_time = time.time()
    
    # Create concurrent write tasks
    tasks = []
    for app_idx in range(applications):
        app_id = f"throughput-{app_idx}"
        for agent_idx in range(writers_per_app):
            tasks.append(write_credit_analysis(
                app_id,
                f"agent-{agent_idx}",
                f"session-{agent_idx}"
            ))
    
    await asyncio.gather(*tasks)
    
    elapsed = time.time() - start_time
    writes_per_second = (applications * writers_per_app) / elapsed
    
    print(f"\n📊 Concurrent Write Throughput:")
    print(f"   Total writes: {applications * writers_per_app}")
    print(f"   Time: {elapsed:.2f}s")
    print(f"   Throughput: {writes_per_second:.0f} writes/sec")
    
    # SLO: >100 writes/sec
    assert writes_per_second > 100, f"Throughput {writes_per_second:.0f} writes/sec below 100 SLO"


@pytest.mark.asyncio
async def test_optimistic_concurrency_conflict_rate(event_store: EventStore):
    """Test that optimistic concurrency conflict rate is within acceptable bounds (<10%)"""
    stream_id = "conflict-rate-test"
    application_id = "conflict-app"
    
    # Create stream
    await event_store.append(
        stream_id,
        [ApplicationSubmitted(
            application_id=application_id,
            applicant_id="test",
            requested_amount_usd=100000,
            business_name="Test Corp",
            tax_id="12-3456789"
        )],
        expected_version=-1
    )
    
    version = await event_store.stream_version(stream_id)
    conflicts = 0
    total_attempts = 100
    
    async def try_write():
        nonlocal conflicts
        try:
            await event_store.append(
                stream_id,
                [CreditAnalysisCompleted(
                    application_id=application_id,
                    risk_tier="MEDIUM",
                    credit_score=750,
                    max_credit_limit=100000,
                    model_version="v1.0"
                )],
                expected_version=version
            )
            return True
        except Exception:
            conflicts += 1
            return False
    
    # Run 100 concurrent attempts
    tasks = [try_write() for _ in range(total_attempts)]
    results = await asyncio.gather(*tasks)
    
    success_count = sum(results)
    conflict_rate = (conflicts / total_attempts) * 100
    
    print(f"\n📊 Optimistic Concurrency Results:")
    print(f"   Successes: {success_count}")
    print(f"   Conflicts: {conflicts}")
    print(f"   Conflict Rate: {conflict_rate:.1f}%")
    
    # SLO: Conflict rate < 10%
    assert conflict_rate < 10, f"Conflict rate {conflict_rate:.1f}% exceeds 10% SLO"