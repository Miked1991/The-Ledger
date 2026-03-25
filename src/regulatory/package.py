# src/regulatory/package.py
import json
import hashlib
from datetime import datetime
from typing import Dict, Any, List
from src.event_store import EventStore
from src.integrity.audit_chain import run_integrity_check
from src.aggregates.loan_application import LoanApplicationAggregate

async def generate_regulatory_package(
    store: EventStore,
    application_id: str,
    examination_date: datetime
) -> Dict[str, Any]:
    """
    Generate a complete regulatory examination package.
    The package is self-contained and can be verified independently.
    """
    # 1. Complete event stream
    stream_id = f"loan-{application_id}"
    events = await store.load_stream(stream_id)
    
    event_stream = []
    for event in events:
        event_stream.append({
            "stream_position": event.stream_position,
            "global_position": event.global_position,
            "event_type": event.event_type,
            "event_version": event.event_version,
            "payload": event.payload,
            "recorded_at": event.recorded_at.isoformat(),
            "correlation_id": event.correlation_id,
            "causation_id": event.causation_id
        })
    
    # 2. Projection state at examination date
    projection_state = await _get_projection_at_date(store, application_id, examination_date)
    
    # 3. Audit chain integrity
    integrity_result = await run_integrity_check(store, "loan", application_id)
    
    # 4. Human-readable narrative
    narrative = await _generate_narrative(events)
    
    # 5. Agent model information
    agent_models = await _extract_agent_models(store, events)
    
    # 6. Package hash for verification
    package_json = {
        "application_id": application_id,
        "examination_date": examination_date.isoformat(),
        "generated_at": datetime.utcnow().isoformat(),
        "event_stream": event_stream,
        "projection_state": projection_state,
        "integrity_check": integrity_result.to_dict(),
        "narrative": narrative,
        "agent_models": agent_models
    }
    
    # Generate package hash
    package_str = json.dumps(package_json, sort_keys=True)
    package_hash = hashlib.sha256(package_str.encode()).hexdigest()
    package_json["package_hash"] = package_hash
    
    return package_json

async def _get_projection_at_date(
    store: EventStore,
    application_id: str,
    examination_date: datetime
) -> Dict[str, Any]:
    """Get projection state as it existed at examination date"""
    # Replay events up to examination date
    stream_id = f"loan-{application_id}"
    events = await store.load_stream(stream_id)
    
    agg = LoanApplicationAggregate(application_id)
    
    for event in events:
        if event.recorded_at <= examination_date:
            agg.apply_event(event)
    
    return {
        "state": agg.state.value,
        "applicant_id": agg.applicant_id,
        "requested_amount": agg.requested_amount,
        "approved_amount": agg.approved_amount,
        "credit_analysis_tier": agg.credit_analysis_tier,
        "fraud_score": agg.fraud_score,
        "final_decision": agg.final_decision
    }

async def _generate_narrative(events: List[StoredEvent]) -> List[str]:
    """Generate human-readable narrative from events"""
    narrative = []
    
    for event in events:
        timestamp = event.recorded_at.strftime("%Y-%m-%d %H:%M:%S")
        
        if event.event_type == "ApplicationSubmitted":
            narrative.append(
                f"[{timestamp}] Application submitted by {event.payload['applicant_id']} "
                f"for ${event.payload['requested_amount_usd']:,.2f}"
            )
        
        elif event.event_type == "CreditAnalysisCompleted":
            narrative.append(
                f"[{timestamp}] Credit analysis completed: risk tier {event.payload['risk_tier']}, "
                f"score {event.payload['credit_score']}, max limit ${event.payload['max_credit_limit']:,.2f}"
            )
        
        elif event.event_type == "FraudScreeningCompleted":
            narrative.append(
                f"[{timestamp}] Fraud screening: score {event.payload['fraud_score']:.2f}, "
                f"flags: {', '.join(event.payload['flags'])}"
            )
        
        elif event.event_type == "ComplianceRulePassed":
            narrative.append(
                f"[{timestamp}] Compliance check passed: {event.payload['rule_id']}"
            )
        
        elif event.event_type == "DecisionGenerated":
            narrative.append(
                f"[{timestamp}] AI decision generated: {event.payload['recommendation']} "
                f"with confidence {event.payload['confidence_score']:.2f}"
            )
        
        elif event.event_type == "HumanReviewCompleted":
            narrative.append(
                f"[{timestamp}] Human review by {event.payload['reviewer_id']}: "
                f"{event.payload['final_decision']}"
                f"{' (OVERRIDE)' if event.payload.get('override') else ''}"
            )
    
    return narrative

async def _extract_agent_models(
    store: EventStore,
    events: List[StoredEvent]
) -> List[Dict[str, Any]]:
    """Extract agent model information from events"""
    agent_models = []
    
    for event in events:
        if event.event_type == "CreditAnalysisCompleted":
            agent_models.append({
                "agent": "CreditAnalysis",
                "model_version": event.payload.get("model_version", "unknown"),
                "confidence_score": event.payload.get("confidence_score"),
                "timestamp": event.recorded_at.isoformat()
            })
        
        elif event.event_type == "DecisionGenerated":
            agent_models.append({
                "agent": "DecisionOrchestrator",
                "model_versions": event.payload.get("model_versions", {}),
                "confidence_score": event.payload.get("confidence_score"),
                "timestamp": event.recorded_at.isoformat()
            })
    
    return agent_models