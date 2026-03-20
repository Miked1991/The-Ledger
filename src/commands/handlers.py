"""
ledger/commands/handlers.py — Command Handlers
===============================================
Implements the load → validate → determine → append pattern for all commands.

Each handler:
1. LOAD: Reconstruct aggregate(s) from event store
2. VALIDATE: Check business rules (using aggregate methods)
3. DETERMINE: Create new event(s) based on command
4. APPEND: Atomically append to store with OCC

Stream naming convention (from canonical schema):
- LoanApplication:   "loan-{application_id}"
- AgentSession:      "agent-{agent_type}-{session_id}"
- CreditRecord:      "credit-{application_id}"
- ComplianceRecord:  "compliance-{application_id}"
- FraudScreening:    "fraud-{application_id}"
- DocumentPackage:   "docpkg-{application_id}"
- AuditLedger:       "audit-{entity_id}"
"""

from __future__ import annotations
from datetime import datetime
from decimal import Decimal
from typing import Optional, List, Dict, Any
import uuid

# Import aggregates
from ledger.aggregates.loan_application import LoanApplicationAggregate
from ledger.aggregates.agent_session import AgentSessionAggregate
from ledger.aggregates.compliance_record import ComplianceRecordAggregate
from ledger.aggregates.credit_record import CreditRecordAggregate
from ledger.aggregates.fraud_screening import FraudScreeningAggregate

# Import canonical events
from ledger.schema.events import (
    # Loan Application Events
    ApplicationSubmitted,
    CreditAnalysisRequested,
    DecisionRequested,
    DecisionGenerated,
    HumanReviewRequested,
    HumanReviewCompleted,
    ApplicationApproved,
    ApplicationDeclined,
    
    # Credit Record Events
    CreditAnalysisCompleted,
    CreditDecision,
    
    # Agent Session Events
    AgentSessionStarted,
    AgentSessionCompleted,
    AgentNodeExecuted,
    AgentToolCalled,
    AgentOutputWritten,
    
    # Compliance Events
    ComplianceCheckInitiated,
    ComplianceRulePassed,
    ComplianceRuleFailed,
    ComplianceCheckCompleted,
    ComplianceVerdict,
    
    # Fraud Events
    FraudScreeningInitiated,
    FraudAnomalyDetected,
    FraudScreeningCompleted,
    
    # Enums
    AgentType,
    RiskTier,
    LoanPurpose,
    FraudAnomalyType
)

# Import exceptions
from ledger.event_store import OptimisticConcurrencyError, DomainError, PreconditionFailed


class CommandHandlers:
    """
    Command handlers following load → validate → determine → append pattern.
    
    Each handler is stateless and idempotent when combined with OCC.
    All business logic is delegated to aggregates.
    """
    
    def __init__(self, event_store):
        self.store = event_store
    
    # ======================================================================
    # Loan Application Commands
    # ======================================================================
    
    async def handle_submit_application(
        self,
        application_id: str,
        applicant_id: str,
        requested_amount_usd: Decimal,
        loan_purpose: LoanPurpose,
        loan_term_months: int,
        submission_channel: str,
        contact_email: str,
        contact_name: str,
        application_reference: str,
        correlation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle ApplicationSubmitted command.
        
        LOAD: Check if application already exists
        VALIDATE: No validation needed for new app (ensured by OCC)
        DETERMINE: Create ApplicationSubmitted event
        APPEND: To loan-{application_id} stream with expected_version=-1
        """
        stream_id = f"loan-{application_id}"
        
        # LOAD: Check if stream already exists
        version = await self.store.stream_version(stream_id)
        if version != -1:
            raise DomainError(f"Application {application_id} already exists")
        
        # DETERMINE: Create event
        event = ApplicationSubmitted(
            event_type="ApplicationSubmitted",
            event_version=1,
            event_id=uuid.uuid4(),
            recorded_at=datetime.utcnow(),
            application_id=application_id,
            applicant_id=applicant_id,
            requested_amount_usd=requested_amount_usd,
            loan_purpose=loan_purpose,
            loan_term_months=loan_term_months,
            submission_channel=submission_channel,
            contact_email=contact_email,
            contact_name=contact_name,
            application_reference=application_reference
        )
        
        # APPEND: New stream
        positions = await self.store.append(
            stream_id=stream_id,
            events=[event.to_store_dict()],
            expected_version=-1,
            causation_id=correlation_id,
            metadata={"command": "submit_application"}
        )
        
        return {
            "application_id": application_id,
            "stream_id": stream_id,
            "position": positions[0] if positions else None,
            "status": "submitted"
        }
    
    async def handle_request_credit_analysis(
        self,
        application_id: str,
        requested_by: str,
        priority: str = "NORMAL",
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle CreditAnalysisRequested command.
        
        LOAD: LoanApplication aggregate
        VALIDATE: Application is in correct state for credit analysis
        DETERMINE: Create CreditAnalysisRequested event
        APPEND: To loan-{application_id} stream
        """
        stream_id = f"loan-{application_id}"
        
        # LOAD: Reconstruct aggregate
        loan_app = await LoanApplicationAggregate.load_from_stream(self.store, stream_id)
        
        # VALIDATE: Business rules
        loan_app.assert_can_request_credit_analysis()
        
        # DETERMINE: Create event
        event = CreditAnalysisRequested(
            event_type="CreditAnalysisRequested",
            event_version=1,
            event_id=uuid.uuid4(),
            recorded_at=datetime.utcnow(),
            application_id=application_id,
            requested_at=datetime.utcnow(),
            requested_by=requested_by,
            priority=priority
        )
        
        # APPEND: With OCC
        positions = await self.store.append(
            stream_id=stream_id,
            events=[event.to_store_dict()],
            expected_version=loan_app.version,
            causation_id=causation_id or correlation_id,
            metadata={"command": "request_credit_analysis"}
        )
        
        return {
            "application_id": application_id,
            "requested_at": event.requested_at.isoformat(),
            "position": positions[0] if positions else None
        }
    
    async def handle_request_decision(
        self,
        application_id: str,
        triggered_by_event_id: str,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle DecisionRequested command.
        
        LOAD: LoanApplication aggregate
        VALIDATE: All analyses are complete
        DETERMINE: Create DecisionRequested event
        APPEND: To loan-{application_id} stream
        """
        stream_id = f"loan-{application_id}"
        
        # LOAD aggregates
        loan_app = await LoanApplicationAggregate.load_from_stream(self.store, stream_id)
        credit_record = await CreditRecordAggregate.load(self.store, application_id)
        compliance_record = await ComplianceRecordAggregate.load(self.store, application_id)
        fraud_record = await FraudScreeningAggregate.load(self.store, application_id)
        
        # VALIDATE: All required analyses are complete
        loan_app.assert_can_generate_decision(
            credit_complete=credit_record.is_complete(),
            compliance_complete=compliance_record.is_complete(),
            fraud_complete=fraud_record.is_complete()
        )
        
        # DETERMINE: Create event
        event = DecisionRequested(
            event_type="DecisionRequested",
            event_version=1,
            event_id=uuid.uuid4(),
            recorded_at=datetime.utcnow(),
            application_id=application_id,
            requested_at=datetime.utcnow(),
            all_analyses_complete=True,
            triggered_by_event_id=triggered_by_event_id
        )
        
        # APPEND
        positions = await self.store.append(
            stream_id=stream_id,
            events=[event.to_store_dict()],
            expected_version=loan_app.version,
            causation_id=causation_id or correlation_id,
            metadata={"command": "request_decision"}
        )
        
        return {
            "application_id": application_id,
            "requested_at": event.requested_at.isoformat(),
            "position": positions[0] if positions else None
        }
    
    async def handle_human_review(
        self,
        application_id: str,
        reviewer_id: str,
        decision_event_id: str,
        assigned_to: Optional[str] = None,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle HumanReviewRequested command.
        
        LOAD: LoanApplication aggregate
        VALIDATE: Decision has been generated
        DETERMINE: Create HumanReviewRequested event
        APPEND: To loan-{application_id} stream
        """
        stream_id = f"loan-{application_id}"
        
        # LOAD
        loan_app = await LoanApplicationAggregate.load_from_stream(self.store, stream_id)
        
        # VALIDATE
        loan_app.assert_can_request_human_review()
        
        # DETERMINE
        event = HumanReviewRequested(
            event_type="HumanReviewRequested",
            event_version=1,
            event_id=uuid.uuid4(),
            recorded_at=datetime.utcnow(),
            application_id=application_id,
            reason="AI decision requires human review",
            decision_event_id=decision_event_id,
            assigned_to=assigned_to,
            requested_at=datetime.utcnow()
        )
        
        # APPEND
        positions = await self.store.append(
            stream_id=stream_id,
            events=[event.to_store_dict()],
            expected_version=loan_app.version,
            causation_id=causation_id or correlation_id,
            metadata={"command": "request_human_review"}
        )
        
        return {
            "application_id": application_id,
            "review_requested": True,
            "assigned_to": assigned_to,
            "position": positions[0] if positions else None
        }
    
    async def handle_human_review_completed(
        self,
        application_id: str,
        reviewer_id: str,
        override: bool,
        original_recommendation: str,
        final_decision: str,
        override_reason: Optional[str] = None,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle HumanReviewCompleted command.
        
        LOAD: LoanApplication aggregate
        VALIDATE: Override requires reason
        DETERMINE: Create HumanReviewCompleted event
        APPEND: To loan-{application_id} stream
        """
        stream_id = f"loan-{application_id}"
        
        # LOAD
        loan_app = await LoanApplicationAggregate.load_from_stream(self.store, stream_id)
        
        # VALIDATE
        if override and not override_reason:
            raise DomainError("Override reason required when override=True")
        
        # DETERMINE
        event = HumanReviewCompleted(
            event_type="HumanReviewCompleted",
            event_version=1,
            event_id=uuid.uuid4(),
            recorded_at=datetime.utcnow(),
            application_id=application_id,
            reviewer_id=reviewer_id,
            override=override,
            original_recommendation=original_recommendation,
            final_decision=final_decision,
            override_reason=override_reason,
            reviewed_at=datetime.utcnow()
        )
        
        # APPEND
        positions = await self.store.append(
            stream_id=stream_id,
            events=[event.to_store_dict()],
            expected_version=loan_app.version,
            causation_id=causation_id or correlation_id,
            metadata={"command": "complete_human_review"}
        )
        
        return {
            "application_id": application_id,
            "final_decision": final_decision,
            "override": override,
            "position": positions[0] if positions else None
        }
    
    # ======================================================================
    # Credit Analysis Commands
    # ======================================================================
    
    async def handle_credit_analysis_completed(
        self,
        application_id: str,
        session_id: str,
        agent_id: str,
        risk_tier: RiskTier,
        recommended_limit_usd: Decimal,
        confidence: float,
        rationale: str,
        model_version: str,
        model_deployment_id: str,
        input_data_hash: str,
        analysis_duration_ms: int,
        key_concerns: Optional[List[str]] = None,
        data_quality_caveats: Optional[List[str]] = None,
        policy_overrides: Optional[List[str]] = None,
        regulatory_basis: Optional[List[str]] = None,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle CreditAnalysisCompleted command.
        
        LOAD: 
            - AgentSession aggregate (for Gas Town validation)
            - CreditRecord aggregate
        VALIDATE:
            - Agent session has context loaded (Gas Town)
            - Model version matches session
            - Credit record can accept analysis
        DETERMINE: Create CreditAnalysisCompleted event
        APPEND: To credit-{application_id} stream
        """
        credit_stream = f"credit-{application_id}"
        agent_stream = f"agent-{AgentType.CREDIT_ANALYSIS.value}-{session_id}"
        
        # LOAD aggregates
        agent_session = await AgentSessionAggregate.load_from_stream(self.store, agent_stream)
        credit_record = await CreditRecordAggregate.load(self.store, application_id)
        
        # VALIDATE: Gas Town pattern
        agent_session.assert_context_loaded()
        agent_session.assert_model_version_current(model_version)
        
        # Create decision value object
        decision = CreditDecision(
            risk_tier=risk_tier,
            recommended_limit_usd=recommended_limit_usd,
            confidence=confidence,
            rationale=rationale,
            key_concerns=key_concerns or [],
            data_quality_caveats=data_quality_caveats or [],
            policy_overrides_applied=policy_overrides or []
        )
        
        # DETERMINE: Create event
        event = CreditAnalysisCompleted(
            event_type="CreditAnalysisCompleted",
            event_version=2,  # v2 has model_versions
            event_id=uuid.uuid4(),
            recorded_at=datetime.utcnow(),
            application_id=application_id,
            session_id=session_id,
            decision=decision,
            model_version=model_version,
            model_deployment_id=model_deployment_id,
            input_data_hash=input_data_hash,
            analysis_duration_ms=analysis_duration_ms,
            regulatory_basis=regulatory_basis or [],
            completed_at=datetime.utcnow()
        )
        
        # APPEND to credit stream
        positions = await self.store.append(
            stream_id=credit_stream,
            events=[event.to_store_dict()],
            expected_version=credit_record.version,
            causation_id=causation_id or correlation_id,
            metadata={
                "command": "credit_analysis_completed",
                "agent_id": agent_id,
                "session_id": session_id
            }
        )
        
        # Also record agent output
        await self._record_agent_output(
            session_id=session_id,
            agent_type=AgentType.CREDIT_ANALYSIS,
            application_id=application_id,
            events_written=[event.to_store_dict()],
            output_summary=f"Credit analysis completed: {risk_tier.value} risk tier",
            correlation_id=correlation_id
        )
        
        return {
            "application_id": application_id,
            "risk_tier": risk_tier.value,
            "confidence": confidence,
            "position": positions[0] if positions else None
        }
    
    # ======================================================================
    # Compliance Commands
    # ======================================================================
    
    async def handle_compliance_check_initiated(
        self,
        application_id: str,
        session_id: str,
        regulation_set_version: str,
        rules_to_evaluate: List[str],
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle ComplianceCheckInitiated command.
        """
        compliance_stream = f"compliance-{application_id}"
        agent_stream = f"agent-{AgentType.COMPLIANCE.value}-{session_id}"
        
        # LOAD
        agent_session = await AgentSessionAggregate.load_from_stream(self.store, agent_stream)
        compliance_record = await ComplianceRecordAggregate.load(self.store, application_id)
        
        # VALIDATE: Gas Town
        agent_session.assert_context_loaded()
        
        # DETERMINE
        event = ComplianceCheckInitiated(
            event_type="ComplianceCheckInitiated",
            event_version=1,
            event_id=uuid.uuid4(),
            recorded_at=datetime.utcnow(),
            application_id=application_id,
            session_id=session_id,
            regulation_set_version=regulation_set_version,
            rules_to_evaluate=rules_to_evaluate,
            initiated_at=datetime.utcnow()
        )
        
        # APPEND
        positions = await self.store.append(
            stream_id=compliance_stream,
            events=[event.to_store_dict()],
            expected_version=compliance_record.version,
            causation_id=causation_id or correlation_id,
            metadata={"command": "initiate_compliance_check"}
        )
        
        return {
            "application_id": application_id,
            "rules_to_check": len(rules_to_evaluate),
            "position": positions[0] if positions else None
        }
    
    async def handle_compliance_rule_result(
        self,
        application_id: str,
        session_id: str,
        rule_id: str,
        rule_name: str,
        rule_version: str,
        passed: bool,
        failure_reason: Optional[str] = None,
        is_hard_block: bool = False,
        remediation_available: bool = False,
        remediation_description: Optional[str] = None,
        evidence_hash: Optional[str] = None,
        note_type: Optional[str] = None,
        note_text: Optional[str] = None,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle compliance rule result (passed/failed/noted).
        """
        compliance_stream = f"compliance-{application_id}"
        
        # LOAD
        compliance_record = await ComplianceRecordAggregate.load(self.store, application_id)
        
        # DETERMINE appropriate event type
        if note_type and note_text:
            event = ComplianceRuleNoted(
                event_type="ComplianceRuleNoted",
                event_version=1,
                event_id=uuid.uuid4(),
                recorded_at=datetime.utcnow(),
                application_id=application_id,
                session_id=session_id,
                rule_id=rule_id,
                rule_name=rule_name,
                note_type=note_type,
                note_text=note_text,
                evaluated_at=datetime.utcnow()
            )
        elif passed:
            event = ComplianceRulePassed(
                event_type="ComplianceRulePassed",
                event_version=1,
                event_id=uuid.uuid4(),
                recorded_at=datetime.utcnow(),
                application_id=application_id,
                session_id=session_id,
                rule_id=rule_id,
                rule_name=rule_name,
                rule_version=rule_version,
                evidence_hash=evidence_hash or "",
                evaluation_notes="",
                evaluated_at=datetime.utcnow()
            )
        else:
            event = ComplianceRuleFailed(
                event_type="ComplianceRuleFailed",
                event_version=1,
                event_id=uuid.uuid4(),
                recorded_at=datetime.utcnow(),
                application_id=application_id,
                session_id=session_id,
                rule_id=rule_id,
                rule_name=rule_name,
                rule_version=rule_version,
                failure_reason=failure_reason or "Unknown",
                is_hard_block=is_hard_block,
                remediation_available=remediation_available,
                remediation_description=remediation_description,
                evidence_hash=evidence_hash or "",
                evaluated_at=datetime.utcnow()
            )
        
        # APPEND
        positions = await self.store.append(
            stream_id=compliance_stream,
            events=[event.to_store_dict()],
            expected_version=compliance_record.version,
            causation_id=causation_id or correlation_id,
            metadata={"command": "record_compliance_rule"}
        )
        
        return {
            "application_id": application_id,
            "rule_id": rule_id,
            "result": "passed" if passed else "failed" if not note_type else "noted",
            "position": positions[0] if positions else None
        }
    
    async def handle_compliance_check_completed(
        self,
        application_id: str,
        session_id: str,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle ComplianceCheckCompleted command.
        """
        compliance_stream = f"compliance-{application_id}"
        
        # LOAD
        compliance_record = await ComplianceRecordAggregate.load(self.store, application_id)
        
        # Get summary from aggregate
        summary = compliance_record.get_compliance_status()
        
        # DETERMINE verdict
        if summary["has_hard_block"]:
            verdict = ComplianceVerdict.BLOCKED
        elif summary["failed_checks"] > 0:
            verdict = ComplianceVerdict.CONDITIONAL
        else:
            verdict = ComplianceVerdict.CLEAR
        
        # DETERMINE event
        event = ComplianceCheckCompleted(
            event_type="ComplianceCheckCompleted",
            event_version=1,
            event_id=uuid.uuid4(),
            recorded_at=datetime.utcnow(),
            application_id=application_id,
            session_id=session_id,
            rules_evaluated=summary["total_checks"],
            rules_passed=summary["passed_checks"],
            rules_failed=summary["failed_checks"],
            rules_noted=summary.get("noted_checks", 0),
            has_hard_block=summary["has_hard_block"],
            overall_verdict=verdict,
            completed_at=datetime.utcnow()
        )
        
        # APPEND
        positions = await self.store.append(
            stream_id=compliance_stream,
            events=[event.to_store_dict()],
            expected_version=compliance_record.version,
            causation_id=causation_id or correlation_id,
            metadata={"command": "complete_compliance_check"}
        )
        
        return {
            "application_id": application_id,
            "verdict": verdict.value,
            "passed": summary["passed_checks"],
            "failed": summary["failed_checks"],
            "position": positions[0] if positions else None
        }
    
    # ======================================================================
    # Fraud Screening Commands
    # ======================================================================
    
    async def handle_fraud_screening_initiated(
        self,
        application_id: str,
        session_id: str,
        screening_model_version: str,
        triggered_by_event_id: str,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle FraudScreeningInitiated command.
        """
        fraud_stream = f"fraud-{application_id}"
        agent_stream = f"agent-{AgentType.FRAUD_DETECTION.value}-{session_id}"
        
        # LOAD
        agent_session = await AgentSessionAggregate.load_from_stream(self.store, agent_stream)
        fraud_record = await FraudScreeningAggregate.load(self.store, application_id)
        
        # VALIDATE
        agent_session.assert_context_loaded()
        
        # DETERMINE
        event = FraudScreeningInitiated(
            event_type="FraudScreeningInitiated",
            event_version=1,
            event_id=uuid.uuid4(),
            recorded_at=datetime.utcnow(),
            application_id=application_id,
            session_id=session_id,
            screening_model_version=screening_model_version,
            initiated_at=datetime.utcnow()
        )
        
        # APPEND
        positions = await self.store.append(
            stream_id=fraud_stream,
            events=[event.to_store_dict()],
            expected_version=fraud_record.version,
            causation_id=causation_id or correlation_id,
            metadata={"command": "initiate_fraud_screening"}
        )
        
        return {
            "application_id": application_id,
            "position": positions[0] if positions else None
        }
    
    async def handle_fraud_screening_completed(
        self,
        application_id: str,
        session_id: str,
        fraud_score: float,
        risk_level: str,
        anomalies_found: int,
        recommendation: str,
        screening_model_version: str,
        input_data_hash: str,
        anomalies: Optional[List[Dict[str, Any]]] = None,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle FraudScreeningCompleted command.
        """
        fraud_stream = f"fraud-{application_id}"
        
        # LOAD
        fraud_record = await FraudScreeningAggregate.load(self.store, application_id)
        
        # DETERMINE
        event = FraudScreeningCompleted(
            event_type="FraudScreeningCompleted",
            event_version=1,
            event_id=uuid.uuid4(),
            recorded_at=datetime.utcnow(),
            application_id=application_id,
            session_id=session_id,
            fraud_score=fraud_score,
            risk_level=risk_level,
            anomalies_found=anomalies_found,
            recommendation=recommendation,
            screening_model_version=screening_model_version,
            input_data_hash=input_data_hash,
            completed_at=datetime.utcnow()
        )
        
        # APPEND
        positions = await self.store.append(
            stream_id=fraud_stream,
            events=[event.to_store_dict()],
            expected_version=fraud_record.version,
            causation_id=causation_id or correlation_id,
            metadata={"command": "complete_fraud_screening"}
        )
        
        # Also record any anomalies as separate events
        if anomalies:
            for anomaly_data in anomalies:
                from ledger.schema.events import FraudAnomaly
                anomaly_event = FraudAnomalyDetected(
                    event_type="FraudAnomalyDetected",
                    event_version=1,
                    event_id=uuid.uuid4(),
                    recorded_at=datetime.utcnow(),
                    application_id=application_id,
                    session_id=session_id,
                    anomaly=FraudAnomaly(**anomaly_data),
                    detected_at=datetime.utcnow()
                )
                await self.store.append(
                    stream_id=fraud_stream,
                    events=[anomaly_event.to_store_dict()],
                    expected_version=fraud_record.version + 1,  # After completion
                    causation_id=causation_id or correlation_id,
                    metadata={"command": "record_fraud_anomaly"}
                )
        
        return {
            "application_id": application_id,
            "fraud_score": fraud_score,
            "risk_level": risk_level,
            "recommendation": recommendation,
            "position": positions[0] if positions else None
        }
    
    # ======================================================================
    # Agent Session Commands
    # ======================================================================
    
    async def handle_start_agent_session(
        self,
        session_id: str,
        agent_type: AgentType,
        agent_id: str,
        application_id: str,
        model_version: str,
        langgraph_graph_version: str,
        context_source: str,
        context_token_count: int,
        correlation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle AgentSessionStarted command.
        
        This is the Gas Town pattern entry point - every agent session
        must start with this event before any decisions can be made.
        """
        stream_id = f"agent-{agent_type.value}-{session_id}"
        
        # LOAD: Check if session already exists
        version = await self.store.stream_version(stream_id)
        if version != -1:
            raise DomainError(f"Session {session_id} already exists")
        
        # DETERMINE
        event = AgentSessionStarted(
            event_type="AgentSessionStarted",
            event_version=1,
            event_id=uuid.uuid4(),
            recorded_at=datetime.utcnow(),
            session_id=session_id,
            agent_type=agent_type,
            agent_id=agent_id,
            application_id=application_id,
            model_version=model_version,
            langgraph_graph_version=langgraph_graph_version,
            context_source=context_source,
            context_token_count=context_token_count,
            started_at=datetime.utcnow()
        )
        
        # APPEND
        positions = await self.store.append(
            stream_id=stream_id,
            events=[event.to_store_dict()],
            expected_version=-1,
            causation_id=correlation_id,
            metadata={"command": "start_agent_session"}
        )
        
        return {
            "session_id": session_id,
            "agent_type": agent_type.value,
            "stream_id": stream_id,
            "position": positions[0] if positions else None
        }
    
    async def handle_agent_node_executed(
        self,
        session_id: str,
        agent_type: AgentType,
        node_name: str,
        node_sequence: int,
        input_keys: List[str],
        output_keys: List[str],
        llm_called: bool,
        duration_ms: int,
        llm_tokens_input: Optional[int] = None,
        llm_tokens_output: Optional[int] = None,
        llm_cost_usd: Optional[float] = None,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle AgentNodeExecuted command.
        
        Called after EACH node in the LangGraph executes.
        Enables detailed tracing and audit.
        """
        stream_id = f"agent-{agent_type.value}-{session_id}"
        
        # LOAD
        agent_session = await AgentSessionAggregate.load_from_stream(self.store, stream_id)
        
        # VALIDATE
        agent_session.assert_context_loaded()
        
        # DETERMINE
        event = AgentNodeExecuted(
            event_type="AgentNodeExecuted",
            event_version=1,
            event_id=uuid.uuid4(),
            recorded_at=datetime.utcnow(),
            session_id=session_id,
            agent_type=agent_type,
            node_name=node_name,
            node_sequence=node_sequence,
            input_keys=input_keys,
            output_keys=output_keys,
            llm_called=llm_called,
            llm_tokens_input=llm_tokens_input,
            llm_tokens_output=llm_tokens_output,
            llm_cost_usd=llm_cost_usd,
            duration_ms=duration_ms,
            executed_at=datetime.utcnow()
        )
        
        # APPEND
        positions = await self.store.append(
            stream_id=stream_id,
            events=[event.to_store_dict()],
            expected_version=agent_session.version,
            causation_id=causation_id or correlation_id,
            metadata={"command": "record_node_execution"}
        )
        
        return {
            "session_id": session_id,
            "node_name": node_name,
            "duration_ms": duration_ms,
            "position": positions[0] if positions else None
        }
    
    async def handle_agent_session_completed(
        self,
        session_id: str,
        agent_type: AgentType,
        application_id: str,
        total_nodes_executed: int,
        total_llm_calls: int,
        total_tokens_used: int,
        total_cost_usd: float,
        total_duration_ms: int,
        next_agent_triggered: Optional[str] = None,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle AgentSessionCompleted command.
        """
        stream_id = f"agent-{agent_type.value}-{session_id}"
        
        # LOAD
        agent_session = await AgentSessionAggregate.load_from_stream(self.store, stream_id)
        
        # VALIDATE
        agent_session.assert_context_loaded()
        
        # DETERMINE
        event = AgentSessionCompleted(
            event_type="AgentSessionCompleted",
            event_version=1,
            event_id=uuid.uuid4(),
            recorded_at=datetime.utcnow(),
            session_id=session_id,
            agent_type=agent_type,
            application_id=application_id,
            total_nodes_executed=total_nodes_executed,
            total_llm_calls=total_llm_calls,
            total_tokens_used=total_tokens_used,
            total_cost_usd=total_cost_usd,
            total_duration_ms=total_duration_ms,
            next_agent_triggered=next_agent_triggered,
            completed_at=datetime.utcnow()
        )
        
        # APPEND
        positions = await self.store.append(
            stream_id=stream_id,
            events=[event.to_store_dict()],
            expected_version=agent_session.version,
            causation_id=causation_id or correlation_id,
            metadata={"command": "complete_agent_session"}
        )
        
        return {
            "session_id": session_id,
            "total_duration_ms": total_duration_ms,
            "total_cost_usd": total_cost_usd,
            "position": positions[0] if positions else None
        }
    
    # ======================================================================
    # Decision Orchestration Commands
    # ======================================================================
    
    async def handle_generate_decision(
        self,
        application_id: str,
        orchestrator_session_id: str,
        recommendation: str,
        confidence: float,
        approved_amount_usd: Optional[Decimal],
        executive_summary: str,
        conditions: Optional[List[str]] = None,
        key_risks: Optional[List[str]] = None,
        contributing_sessions: Optional[List[str]] = None,
        model_versions: Optional[Dict[str, str]] = None,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle DecisionGenerated command.
        
        LOAD:
            - LoanApplication aggregate
            - All contributing agent sessions (for causal chain)
        VALIDATE:
            - All contributing sessions actually processed this app
            - Confidence floor (if < 0.6, recommendation must be "REFER")
        DETERMINE: Create DecisionGenerated event
        APPEND: To loan-{application_id} stream
        """
        stream_id = f"loan-{application_id}"
        
        # LOAD
        loan_app = await LoanApplicationAggregate.load_from_stream(self.store, stream_id)
        
        # VALIDATE: Confidence floor (regulatory requirement)
        if confidence < 0.6 and recommendation != "REFER":
            raise DomainError(
                f"Confidence {confidence} < 0.6 requires recommendation='REFER', "
                f"got '{recommendation}'"
            )
        
        # VALIDATE: Causal chain - all contributing sessions must have processed this app
        if contributing_sessions:
            for session_ref in contributing_sessions:
                # Session ref could be "agent-credit_analysis-sess-123" or just "sess-123"
                if session_ref.startswith("agent-"):
                    # Full stream ID
                    agent_session = await AgentSessionAggregate.load_from_stream(
                        self.store, session_ref
                    )
                else:
                    # Just session ID - need to find it
                    # This is simplified - in production you'd have a session registry
                    pass
                
                # Check if this session processed this application
                # This would need the aggregate to track processed apps
                # Simplified for now
        
        # DETERMINE
        event = DecisionGenerated(
            event_type="DecisionGenerated",
            event_version=2,  # v2 has model_versions
            event_id=uuid.uuid4(),
            recorded_at=datetime.utcnow(),
            application_id=application_id,
            orchestrator_session_id=orchestrator_session_id,
            recommendation=recommendation,
            confidence=confidence,
            approved_amount_usd=approved_amount_usd,
            conditions=conditions or [],
            executive_summary=executive_summary,
            key_risks=key_risks or [],
            contributing_sessions=contributing_sessions or [],
            model_versions=model_versions or {},
            generated_at=datetime.utcnow()
        )
        
        # APPEND
        positions = await self.store.append(
            stream_id=stream_id,
            events=[event.to_store_dict()],
            expected_version=loan_app.version,
            causation_id=causation_id or correlation_id,
            metadata={"command": "generate_decision"}
        )
        
        return {
            "application_id": application_id,
            "recommendation": recommendation,
            "confidence": confidence,
            "position": positions[0] if positions else None
        }
    
    # ======================================================================
    # Final Decision Commands (after human review)
    # ======================================================================
    
    async def handle_approve_application(
        self,
        application_id: str,
        approved_amount_usd: Decimal,
        interest_rate_pct: float,
        term_months: int,
        approved_by: str,
        effective_date: str,
        conditions: Optional[List[str]] = None,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle ApplicationApproved command.
        """
        stream_id = f"loan-{application_id}"
        
        # LOAD
        loan_app = await LoanApplicationAggregate.load_from_stream(self.store, stream_id)
        
        # VALIDATE
        loan_app.assert_can_approve()
        
        # DETERMINE
        event = ApplicationApproved(
            event_type="ApplicationApproved",
            event_version=1,
            event_id=uuid.uuid4(),
            recorded_at=datetime.utcnow(),
            application_id=application_id,
            approved_amount_usd=approved_amount_usd,
            interest_rate_pct=interest_rate_pct,
            term_months=term_months,
            conditions=conditions or [],
            approved_by=approved_by,
            effective_date=effective_date,
            approved_at=datetime.utcnow()
        )
        
        # APPEND
        positions = await self.store.append(
            stream_id=stream_id,
            events=[event.to_store_dict()],
            expected_version=loan_app.version,
            causation_id=causation_id or correlation_id,
            metadata={"command": "approve_application"}
        )
        
        return {
            "application_id": application_id,
            "status": "APPROVED",
            "approved_amount": float(approved_amount_usd),
            "position": positions[0] if positions else None
        }
    
    async def handle_decline_application(
        self,
        application_id: str,
        decline_reasons: List[str],
        declined_by: str,
        adverse_action_notice_required: bool = True,
        adverse_action_codes: Optional[List[str]] = None,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle ApplicationDeclined command.
        """
        stream_id = f"loan-{application_id}"
        
        # LOAD
        loan_app = await LoanApplicationAggregate.load_from_stream(self.store, stream_id)
        
        # VALIDATE
        loan_app.assert_can_decline()
        
        # DETERMINE
        event = ApplicationDeclined(
            event_type="ApplicationDeclined",
            event_version=1,
            event_id=uuid.uuid4(),
            recorded_at=datetime.utcnow(),
            application_id=application_id,
            decline_reasons=decline_reasons,
            declined_by=declined_by,
            adverse_action_notice_required=adverse_action_notice_required,
            adverse_action_codes=adverse_action_codes or [],
            declined_at=datetime.utcnow()
        )
        
        # APPEND
        positions = await self.store.append(
            stream_id=stream_id,
            events=[event.to_store_dict()],
            expected_version=loan_app.version,
            causation_id=causation_id or correlation_id,
            metadata={"command": "decline_application"}
        )
        
        return {
            "application_id": application_id,
            "status": "DECLINED",
            "reasons": decline_reasons,
            "position": positions[0] if positions else None
        }
    
    # ======================================================================
    # Private Helpers
    # ======================================================================
    
    async def _record_agent_output(
        self,
        session_id: str,
        agent_type: AgentType,
        application_id: str,
        events_written: List[Dict[str, Any]],
        output_summary: str,
        correlation_id: Optional[str] = None
    ) -> None:
        """
        Record that an agent wrote output events.
        
        This maintains the causal chain from agent execution to domain events.
        """
        stream_id = f"agent-{agent_type.value}-{session_id}"
        
        # LOAD current session version
        version = await self.store.stream_version(stream_id)
        
        # DETERMINE
        event = AgentOutputWritten(
            event_type="AgentOutputWritten",
            event_version=1,
            event_id=uuid.uuid4(),
            recorded_at=datetime.utcnow(),
            session_id=session_id,
            agent_type=agent_type,
            application_id=application_id,
            events_written=events_written,
            output_summary=output_summary,
            written_at=datetime.utcnow()
        )
        
        # APPEND
        await self.store.append(
            stream_id=stream_id,
            events=[event.to_store_dict()],
            expected_version=version,
            causation_id=correlation_id,
            metadata={"command": "record_agent_output"}
        )