# tests/test_audit_chain.py
"""
Comprehensive integrity and tamper detection tests.
"""

import pytest
import pytest_asyncio
import asyncpg
import json
import hashlib
from src.event_store import EventStore
from src.integrity.audit_chain import run_integrity_check, IntegrityCheckResult
from src.aggregates.audit_ledger import AuditLedgerAggregate
from src.models.events import ApplicationSubmitted, CreditAnalysisCompleted


@pytest.mark.asyncio
async def test_audit_chain_initialization(event_store: EventStore):
    """Test audit chain initialization"""
    application_id = "test-audit-init"
    
    # Create application events
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
    
    # Run integrity check
    result = await run_integrity_check(event_store, "loan", application_id)
    
    assert isinstance(result, IntegrityCheckResult)
    assert result.events_verified == 1
    assert result.chain_valid is True
    assert result.tamper_detected is False
    assert result.previous_hash == "0" * 64
    assert len(result.current_hash) == 64


@pytest.mark.asyncio
async def test_audit_chain_multiple_events(event_store: EventStore):
    """Test audit chain with multiple events"""
    application_id = "test-audit-multi"
    stream_id = f"loan-{application_id}"
    
    # Create multiple events
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
    
    await event_store.append(
        stream_id,
        [CreditAnalysisCompleted(
            application_id=application_id,
            risk_tier="MEDIUM",
            credit_score=750,
            max_credit_limit=100000,
            model_version="v1.0"
        )],
        expected_version=1
    )
    
    # First integrity check
    result1 = await run_integrity_check(event_store, "loan", application_id)
    assert result1.events_verified == 2
    assert result1.chain_valid is True
    
    # Add another event
    await event_store.append(
        stream_id,
        [CreditAnalysisCompleted(
            application_id=application_id,
            risk_tier="HIGH",
            credit_score=700,
            max_credit_limit=80000,
            model_version="v1.0"
        )],
        expected_version=2
    )
    
    # Second integrity check
    result2 = await run_integrity_check(event_store, "loan", application_id)
    assert result2.events_verified == 3
    assert result2.chain_valid is True
    
    # Verify hash chain continuity
    assert result2.previous_hash != "0" * 64
    assert result2.previous_hash == result1.current_hash


@pytest.mark.asyncio
async def test_tamper_detection(event_store: EventStore, db_pool: asyncpg.Pool):
    """Test that tampering with stored events is detected"""
    application_id = "test-tamper"
    stream_id = f"loan-{application_id}"
    
    # Create events
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
    
    await event_store.append(
        stream_id,
        [CreditAnalysisCompleted(
            application_id=application_id,
            risk_tier="MEDIUM",
            credit_score=750,
            max_credit_limit=100000,
            model_version="v1.0"
        )],
        expected_version=1
    )
    
    # Run initial integrity check (establishes baseline)
    initial_result = await run_integrity_check(event_store, "loan", application_id)
    assert initial_result.chain_valid is True
    
    # Tamper with an event (simulate database modification)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            UPDATE events 
            SET payload = payload || '{"tampered": true}'::jsonb
            WHERE stream_id = $1 AND stream_position = 2
        """, stream_id)
    
    # Run integrity check again - should detect tampering
    tamper_result = await run_integrity_check(event_store, "loan", application_id)
    
    # Verify tamper detection
    assert tamper_result.chain_valid is False, "Tampered chain should be invalid"
    assert tamper_result.tamper_detected is True, "Tampering should be detected"
    
    # Verify the integrity check event records tampering
    audit_stream = f"audit-loan-{application_id}"
    audit_events = await event_store.load_stream(audit_stream)
    
    # The last event should have tamper_detected=True
    last_event = audit_events[-1]
    assert last_event.payload.get("tamper_detected") is True
    assert last_event.payload.get("chain_valid") is False


@pytest.mark.asyncio
async def test_hash_chain_continuity(event_store: EventStore):
    """Test that hash chain maintains continuity across multiple checks"""
    application_id = "test-chain-continuity"
    stream_id = f"loan-{application_id}"
    
    hashes = []
    
    # Create events incrementally and verify chain
    for i in range(5):
        await event_store.append(
            stream_id,
            [CreditAnalysisCompleted(
                application_id=application_id,
                risk_tier="MEDIUM",
                credit_score=750 - i * 10,
                max_credit_limit=100000 - i * 5000,
                model_version="v1.0"
            )],
            expected_version=i
        )
        
        result = await run_integrity_check(event_store, "loan", application_id)
        hashes.append(result.current_hash)
        
        # Each check should have a unique hash
        if i > 0:
            assert hashes[i] != hashes[i-1], "Hash should change with new events"
    
    # Verify chain progression
    for i in range(1, len(hashes)):
        assert hashes[i] != hashes[i-1]


@pytest.mark.asyncio
async def test_multiple_entity_audit_chains(event_store: EventStore):
    """Test independent audit chains for different entity types"""
    applications = ["APP-001", "APP-002", "APP-003"]
    
    results = []
    
    for app_id in applications:
        stream_id = f"loan-{app_id}"
        
        await event_store.append(
            stream_id,
            [ApplicationSubmitted(
                application_id=app_id,
                applicant_id=f"test-{app_id}",
                requested_amount_usd=100000,
                business_name=f"Corp {app_id}",
                tax_id=f"12-{app_id}"
            )],
            expected_version=-1
        )
        
        result = await run_integrity_check(event_store, "loan", app_id)
        results.append(result)
        
        assert result.chain_valid is True
        assert result.events_verified == 1
    
    # Verify each has its own hash chain
    hashes = [r.current_hash for r in results]
    assert len(set(hashes)) == 3, "Each entity should have unique hash chain"


@pytest.mark.asyncio
async def test_audit_chain_replay(event_store: EventStore):
    """Test that audit chain can be reconstructed from events"""
    application_id = "test-replay"
    stream_id = f"loan-{application_id}"
    
    # Create events
    for i in range(10):
        await event_store.append(
            stream_id,
            [CreditAnalysisCompleted(
                application_id=application_id,
                risk_tier="MEDIUM",
                credit_score=750,
                max_credit_limit=100000,
                model_version="v1.0"
            )],
            expected_version=i
        )
    
    # Run integrity check
    result = await run_integrity_check(event_store, "loan", application_id)
    assert result.events_verified == 10
    
    # Verify we can get audit trail
    audit_agg = await AuditLedgerAggregate.load(event_store, "loan", application_id)
    audit_trail = audit_agg.get_audit_trail()
    
    assert audit_trail["entity_type"] == "loan"
    assert audit_trail["entity_id"] == application_id
    assert audit_trail["total_checks"] >= 1
    assert audit_trail["last_hash"] == result.current_hash