# src/commands/handlers.py
"""
Complete Command Handlers for The Ledger Event Store.

Implements all 6 business rules with load → validate → determine → append pattern.
Handles all 4 aggregates: LoanApplication, AgentSession, ComplianceRecord, AuditLedger.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid
import logging

from src.event_store import EventStore
from src.aggregates.loan_application import LoanApplicationAggregate, ApplicationState
from src.aggregates.agent_session import AgentSessionAggregate
from src.aggregates.compliance_record import ComplianceRecordAggregate, ComplianceStatus
from src.aggregates.audit_ledger import AuditLedgerAggregate
from src.models.events import (
    ApplicationSubmitted, CreditAnalysisCompleted, FraudScreeningCompleted,
    ComplianceRulePassed, ComplianceRuleFailed, DecisionGenerated,
    HumanReviewCompleted, AgentContextLoaded, AgentActionTaken,
    ComplianceCheckInitiated, AuditIntegrityCheckRun
)
from src.models.errors import (
    DomainError, PreconditionFailedError, InvalidStateTransitionError,
    OptimisticConcurrencyError
)

logger = logging.getLogger(__name__)


class CommandHandlers:
    """
    Complete Command Handlers for the loan application domain.
    
    Implements the load → validate → determine → append pattern for all commands.
    Enforces all 6 business rules from the specification:
    1. Application state machine (valid transitions only)
    2. Gas Town pattern (agent context requirement)
    3. Model version locking (no analysis churn)
    4. Confidence floor (confidence < 0.6 forces REFER)
    5. Compliance dependency (all checks required for approval)
    6. Causal chain enforcement (agent sessions must reference actual work)
    """
    
    def __init__(self, store: EventStore):
        self.store = store
    
    # =========================================================================
    # Loan Application Commands
    # =========================================================================
    
    async def handle_submit_application(
        self,
        application_id: str,
        applicant_id: str,
        requested_amount: float,
        business_name: str,
        tax_id: str,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> None:
        """
        Handle application submission command.
        
        Business Rules:
        - Application ID must be unique
        - Application must be in SUBMITTED state
        
        Flow: Load → Validate → Determine → Append
        """
        logger.info(f"Handling submit_application for {application_id}")
        
        # 1. Load aggregate (or create new if doesn't exist)
        try:
            app = await LoanApplicationAggregate.load(self.store, application_id)
            # If load succeeds, application already exists
            if app.version > 0:
                raise DomainError(f"Application {application_id} already exists")
        except Exception:
            # Application doesn't exist, create new aggregate
            app = LoanApplicationAggregate(application_id)
        
        # 2. Validate business rules
        app.assert_submit(application_id, applicant_id, requested_amount)
        
        # 3. Determine events
        event = app.create_submitted_event(
            applicant_id=applicant_id,
            requested_amount=requested_amount,
            business_name=business_name,
            tax_id=tax_id
        )
        app.apply_new_event(event)
        
        # 4. Append atomically with optimistic concurrency
        stream_id = f"loan-{application_id}"
        await app.commit(
            self.store,
            stream_id,
            app.version,
            correlation_id=correlation_id,
            causation_id=causation_id
        )
        
        logger.info(f"✓ Application {application_id} submitted successfully")
    
    async def handle_credit_analysis_completed(
        self,
        application_id: str,
        agent_id: str,
        session_id: str,
        risk_tier: str,
        credit_score: int,
        max_credit_limit: float,
        model_version: str,
        confidence_score: Optional[float] = None,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> None:
        """
        Handle credit analysis completion command.
        
        Business Rules:
        - Gas Town: Agent must have context loaded (Rule #2)
        - Model version locking (Rule #3)
        - Application must be in AWAITING_ANALYSIS state
        
        Flow: Load → Validate → Determine → Append
        """
        logger.info(f"Handling credit_analysis_completed for {application_id}")
        
        # 1. Load aggregates
        app = await LoanApplicationAggregate.load(self.store, application_id)
        agent = await AgentSessionAggregate.load(self.store, agent_id, session_id)
        
        # 2. Validate business rules
        app.assert_awaiting_credit_analysis()
        app.assert_credit_analysis_not_completed()
        agent.assert_context_loaded()  # Gas Town pattern
        agent.assert_model_version_current(model_version)  # Model version locking
        
        # 3. Determine events
        credit_event = app.create_credit_analysis_event(
            risk_tier=risk_tier,
            credit_score=credit_score,
            max_credit_limit=max_credit_limit,
            model_version=model_version,
            confidence_score=confidence_score
        )
        app.apply_new_event(credit_event)
        
        # Record agent action
        action_event = agent.create_action_taken_event(
            action_type="credit_analysis",
            input_hash=f"hash_{application_id}_{risk_tier}",
            reasoning_trace=f"Credit analysis completed with {risk_tier} risk tier",
            output_data={
                "risk_tier": risk_tier,
                "credit_score": credit_score,
                "max_credit_limit": max_credit_limit,
                "model_version": model_version
            }
        )
        agent.apply_new_event(action_event)
        
        # 4. Append atomically (both aggregates in separate transactions)
        # First commit loan aggregate
        loan_stream = f"loan-{application_id}"
        await app.commit(
            self.store,
            loan_stream,
            app.version,
            correlation_id=correlation_id,
            causation_id=causation_id or session_id
        )
        
        # Then commit agent aggregate
        agent_stream = f"agent-{agent_id}-{session_id}"
        await agent.commit(
            self.store,
            agent_stream,
            agent.version,
            correlation_id=correlation_id
        )
        
        logger.info(f"✓ Credit analysis recorded for {application_id}")
    
    async def handle_fraud_screening_completed(
        self,
        application_id: str,
        agent_id: str,
        session_id: str,
        fraud_score: float,
        flags: List[str],
        risk_indicators: Dict[str, Any],
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> None:
        """
        Handle fraud screening completion command.
        
        Business Rules:
        - Gas Town: Agent must have context loaded (Rule #2)
        - Fraud score must be between 0.0 and 1.0
        
        Flow: Load → Validate → Determine → Append
        """
        logger.info(f"Handling fraud_screening_completed for {application_id}")
        
        # 1. Load aggregates
        app = await LoanApplicationAggregate.load(self.store, application_id)
        agent = await AgentSessionAggregate.load(self.store, agent_id, session_id)
        
        # 2. Validate business rules
        if fraud_score < 0.0 or fraud_score > 1.0:
            raise DomainError(f"Fraud score {fraud_score} must be between 0.0 and 1.0")
        agent.assert_context_loaded()  # Gas Town pattern
        
        # 3. Determine events
        from src.models.events import FraudScreeningCompleted
        fraud_event = FraudScreeningCompleted(
            application_id=application_id,
            fraud_score=fraud_score,
            flags=flags,
            risk_indicators=risk_indicators
        )
        app.apply_new_event(fraud_event)
        
        # Record agent action
        action_event = agent.create_action_taken_event(
            action_type="fraud_screening",
            input_hash=f"hash_{application_id}_{fraud_score}",
            reasoning_trace=f"Fraud screening completed with score {fraud_score}",
            output_data={
                "fraud_score": fraud_score,
                "flags": flags,
                "risk_indicators": risk_indicators
            }
        )
        agent.apply_new_event(action_event)
        
        # 4. Append atomically
        loan_stream = f"loan-{application_id}"
        await app.commit(
            self.store,
            loan_stream,
            app.version,
            correlation_id=correlation_id,
            causation_id=causation_id or session_id
        )
        
        agent_stream = f"agent-{agent_id}-{session_id}"
        await agent.commit(
            self.store,
            agent_stream,
            agent.version,
            correlation_id=correlation_id
        )
        
        logger.info(f"✓ Fraud screening recorded for {application_id} (score: {fraud_score})")
    
    async def handle_compliance_check(
        self,
        application_id: str,
        rule_id: str,
        regulation_version: str,
        check_id: str,
        passed: bool,
        details: Optional[Dict[str, Any]] = None,
        failure_reason: Optional[str] = None,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> None:
        """
        Handle compliance check recording command.
        
        Business Rules:
        - Compliance dependency: Checks must be recorded in compliance aggregate (Rule #5)
        - Rule ID must exist in active regulation set
        
        Flow: Load → Validate → Determine → Append
        """
        logger.info(f"Handling compliance_check for {application_id} - {rule_id}")
        
        # 1. Load compliance aggregate
        compliance = await ComplianceRecordAggregate.load(self.store, application_id)
        
        # 2. Validate business rules
        if compliance.required_checks and rule_id not in compliance.required_checks:
            compliance.assert_rule_exists(rule_id)
        compliance.assert_no_duplicate_check(rule_id)
        
        # 3. Determine events
        if passed:
            event = ComplianceRulePassed(
                application_id=application_id,
                rule_id=rule_id,
                regulation_version=regulation_version,
                check_id=check_id,
                details=details or {}
            )
        else:
            event = ComplianceRuleFailed(
                application_id=application_id,
                rule_id=rule_id,
                regulation_version=regulation_version,
                check_id=check_id,
                failure_reason=failure_reason or "Compliance check failed",
                details=details or {}
            )
        
        compliance.apply_new_event(event)
        
        # 4. Append atomically
        compliance_stream = f"compliance-{application_id}"
        await compliance.commit(
            self.store,
            compliance_stream,
            compliance.version,
            correlation_id=correlation_id,
            causation_id=causation_id
        )
        
        logger.info(f"✓ Compliance check {rule_id}: {'PASSED' if passed else 'FAILED'}")
    
    async def handle_initiate_compliance_checks(
        self,
        application_id: str,
        required_checks: List[str],
        regulation_version: str,
        initiated_by: str,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> None:
        """
        Handle initiating compliance checks for an application.
        
        Flow: Load → Validate → Determine → Append
        """
        logger.info(f"Handling initiate_compliance_checks for {application_id}")
        
        # 1. Load compliance aggregate
        compliance = await ComplianceRecordAggregate.load(self.store, application_id)
        
        # 2. Validate
        if compliance.required_checks:
            raise DomainError(f"Compliance checks already initiated for {application_id}")
        
        # 3. Determine events
        event = ComplianceCheckInitiated(
            application_id=application_id,
            check_id=f"init-{application_id}-{uuid.uuid4().hex[:8]}",
            required_checks=required_checks,
            regulation_version=regulation_version,
            initiated_by=initiated_by
        )
        compliance.apply_new_event(event)
        
        # 4. Append atomically
        compliance_stream = f"compliance-{application_id}"
        await compliance.commit(
            self.store,
            compliance_stream,
            compliance.version,
            correlation_id=correlation_id,
            causation_id=causation_id
        )
        
        logger.info(f"✓ Compliance checks initiated for {application_id}: {required_checks}")
    
    async def handle_generate_decision(
        self,
        application_id: str,
        agent_id: str,
        session_id: str,
        orchestrator_recommendation: str,
        orchestrator_confidence: float,
        contributing_sessions: List[str],
        reasoning: str,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> None:
        """
        Handle decision generation command.
        
        Business Rules:
        - Gas Town: Agent must have context loaded (Rule #2)
        - Confidence floor: confidence < 0.6 forces REFER (Rule #4)
        - Causal chain: contributing sessions must have processed this application (Rule #6)
        - Compliance dependency: All required checks must be complete (Rule #5)
        
        Flow: Load → Validate → Determine → Append
        """
        logger.info(f"Handling generate_decision for {application_id}")
        
        # 1. Load aggregates
        app = await LoanApplicationAggregate.load(self.store, application_id)
        agent = await AgentSessionAggregate.load(self.store, agent_id, session_id)
        compliance = await ComplianceRecordAggregate.load(self.store, application_id)
        
        # 2. Validate business rules
        app.assert_decision_generation_allowed()
        agent.assert_context_loaded()  # Gas Town pattern
        
        # Rule #5: Compliance dependency - all required checks must be complete
        if compliance.required_checks:
            compliance.assert_compliance_clearance()
        
        # Rule #6: Causal chain enforcement
        for session in contributing_sessions:
            # Parse session ID to get agent_id and session_id
            # Format: agent-{agent_id}-{session_id}
            parts = session.split("-")
            if len(parts) >= 3:
                contrib_agent_id = parts[1]
                contrib_session_id = parts[2]
                
                # Load the contributing agent session
                contrib_agent = await AgentSessionAggregate.load(
                    self.store, contrib_agent_id, contrib_session_id
                )
                
                # Check if this session processed this application
                found_application = False
                for action in contrib_agent.actions_taken:
                    output = action.get("output", {})
                    if output.get("application_id") == application_id:
                        found_application = True
                        break
                
                if not found_application:
                    raise DomainError(
                        f"Session {session} did not process application {application_id}",
                        session=session,
                        application_id=application_id,
                        suggested_action="Verify contributing_sessions list"
                    )
        
        # 3. Determine events
        final_recommendation = orchestrator_recommendation
        
        # Rule #4: Confidence floor enforcement
        if orchestrator_confidence < 0.6:
            final_recommendation = "REFER"
            logger.info(f"Confidence floor triggered: {orchestrator_confidence} < 0.6, forcing REFER")
        
        decision_event = app.create_decision_event(
            recommendation=final_recommendation,
            confidence_score=orchestrator_confidence,
            contributing_sessions=contributing_sessions,
            reasoning=reasoning
        )
        app.apply_new_event(decision_event)
        
        # Record agent action
        action_event = agent.create_action_taken_event(
            action_type="generate_decision",
            input_hash=f"hash_{application_id}_{final_recommendation}",
            reasoning_trace=reasoning,
            output_data={
                "recommendation": final_recommendation,
                "confidence": orchestrator_confidence,
                "contributing_sessions": contributing_sessions
            }
        )
        agent.apply_new_event(action_event)
        
        # 4. Append atomically
        loan_stream = f"loan-{application_id}"
        await app.commit(
            self.store,
            loan_stream,
            app.version,
            correlation_id=correlation_id,
            causation_id=causation_id or session_id
        )
        
        agent_stream = f"agent-{agent_id}-{session_id}"
        await agent.commit(
            self.store,
            agent_stream,
            agent.version,
            correlation_id=correlation_id
        )
        
        logger.info(f"✓ Decision generated for {application_id}: {final_recommendation}")
    
    async def handle_human_review_completed(
        self,
        application_id: str,
        reviewer_id: str,
        final_decision: str,
        override: bool = False,
        override_reason: Optional[str] = None,
        comments: Optional[str] = None,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> None:
        """
        Handle human review completion command.
        
        Business Rules:
        - Application must be in PENDING_HUMAN state
        - Override requires reason if override=True
        
        Flow: Load → Validate → Determine → Append
        """
        logger.info(f"Handling human_review_completed for {application_id}")
        
        # 1. Load aggregate
        app = await LoanApplicationAggregate.load(self.store, application_id)
        
        # 2. Validate
        app.assert_final_decision_allowed()
        
        if override and not override_reason:
            raise DomainError("Override requires a reason")
        
        # 3. Determine events
        event = HumanReviewCompleted(
            application_id=application_id,
            reviewer_id=reviewer_id,
            final_decision=final_decision,
            override=override,
            override_reason=override_reason,
            comments=comments
        )
        app.apply_new_event(event)
        
        # 4. Append atomically
        loan_stream = f"loan-{application_id}"
        await app.commit(
            self.store,
            loan_stream,
            app.version,
            correlation_id=correlation_id,
            causation_id=causation_id
        )
        
        logger.info(f"✓ Human review recorded for {application_id}: {final_decision}")
    
    # =========================================================================
    # Agent Session Commands (Gas Town Pattern)
    # =========================================================================
    
    async def handle_start_agent_session(
        self,
        agent_id: str,
        session_id: str,
        context_source: str,
        model_version: str,
        token_count: int,
        context_hash: str,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> None:
        """
        Handle agent session start command.
        
        Gas Town Pattern: Agents must load context before making decisions.
        This is Business Rule #2.
        
        Flow: Load → Validate → Determine → Append
        """
        logger.info(f"Handling start_agent_session for {agent_id}/{session_id}")
        
        # 1. Load aggregate
        agent = await AgentSessionAggregate.load(self.store, agent_id, session_id)
        
        # 2. Validate - ensure context not already loaded
        if agent.context_loaded:
            raise DomainError(f"Session {session_id} already has context loaded")
        
        # 3. Determine events
        event = agent.create_context_loaded_event(
            context_source=context_source,
            model_version=model_version,
            token_count=token_count,
            context_hash=context_hash
        )
        agent.apply_new_event(event)
        
        # 4. Append atomically
        agent_stream = f"agent-{agent_id}-{session_id}"
        await agent.commit(
            self.store,
            agent_stream,
            agent.version,
            correlation_id=correlation_id,
            causation_id=causation_id
        )
        
        logger.info(f"✓ Agent session {session_id} started with context from {context_source}")
    
    async def handle_record_agent_action(
        self,
        agent_id: str,
        session_id: str,
        action_type: str,
        input_data_hash: str,
        reasoning_trace: str,
        output_data: Dict[str, Any],
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> None:
        """
        Handle recording an agent action.
        
        Gas Town Pattern: Actions require context loaded.
        
        Flow: Load → Validate → Determine → Append
        """
        logger.info(f"Handling record_agent_action for {agent_id}/{session_id}: {action_type}")
        
        # 1. Load aggregate
        agent = await AgentSessionAggregate.load(self.store, agent_id, session_id)
        
        # 2. Validate
        agent.assert_context_loaded()  # Gas Town pattern
        
        # 3. Determine events
        event = agent.create_action_taken_event(
            action_type=action_type,
            input_hash=input_data_hash,
            reasoning_trace=reasoning_trace,
            output_data=output_data
        )
        agent.apply_new_event(event)
        
        # 4. Append atomically
        agent_stream = f"agent-{agent_id}-{session_id}"
        await agent.commit(
            self.store,
            agent_stream,
            agent.version,
            correlation_id=correlation_id,
            causation_id=causation_id
        )
        
        logger.info(f"✓ Agent action {action_type} recorded")
    
    # =========================================================================
    # Compliance Commands
    # =========================================================================
    
    async def handle_compliance_override(
        self,
        application_id: str,
        overridden_by: str,
        reason: str,
        justification: str,
        user_role: str,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> None:
        """
        Handle compliance override command.
        
        Business Rules:
        - Only compliance officers and admins can override
        - Override requires justification
        
        Flow: Load → Validate → Determine → Append
        """
        logger.info(f"Handling compliance_override for {application_id}")
        
        # 1. Load compliance aggregate
        compliance = await ComplianceRecordAggregate.load(self.store, application_id)
        
        # 2. Validate override permissions
        compliance.assert_override_allowed(user_role)
        
        # 3. Create override event
        from src.models.events import BaseEvent
        
        class ComplianceOverride(BaseEvent):
            event_type: str = "ComplianceOverride"
            event_version: int = 1
            application_id: str
            overridden_by: str
            reason: str
            justification: str
        
        event = ComplianceOverride(
            application_id=application_id,
            overridden_by=overridden_by,
            reason=reason,
            justification=justification
        )
        compliance.apply_new_event(event)
        
        # 4. Append atomically
        compliance_stream = f"compliance-{application_id}"
        await compliance.commit(
            self.store,
            compliance_stream,
            compliance.version,
            correlation_id=correlation_id,
            causation_id=causation_id
        )
        
        logger.info(f"✓ Compliance overridden for {application_id} by {overridden_by}")
    
    # =========================================================================
    # Audit Commands
    # =========================================================================
    
    async def handle_run_audit_check(
        self,
        entity_type: str,
        entity_id: str,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle running audit integrity check.
        
        Flow: Load → Validate → Determine → Append
        """
        logger.info(f"Handling run_audit_check for {entity_type}/{entity_id}")
        
        from src.integrity.audit_chain import run_integrity_check
        
        # Run integrity check (this loads events and verifies chain)
        result = await run_integrity_check(self.store, entity_type, entity_id)
        
        # The integrity check automatically appends an AuditIntegrityCheckRun event
        # So no additional commit needed
        
        logger.info(f"✓ Audit check completed for {entity_type}/{entity_id}: {result.chain_valid}")
        
        return result.to_dict()
    
    async def handle_verify_audit_chain(
        self,
        entity_type: str,
        entity_id: str,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle verifying the audit chain without creating a new check event.
        
        Flow: Load → Validate → Determine → Return
        """
        logger.info(f"Handling verify_audit_chain for {entity_type}/{entity_id}")
        
        # Load audit ledger
        audit = await AuditLedgerAggregate.load(self.store, entity_type, entity_id)
        
        # Get events for this entity
        stream_id = f"{entity_type}-{entity_id}"
        events = await self.store.load_stream(stream_id)
        
        # Verify integrity
        is_valid, current_hash, breaks = await audit.verify_integrity(self.store, events)
        
        return {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "chain_valid": is_valid,
            "current_hash": current_hash,
            "chain_breaks": breaks,
            "events_verified": len(events),
            "last_check": audit.get_integrity_report()
        }
    
    # =========================================================================
    # Batch Operations
    # =========================================================================
    
    async def handle_batch_credit_analysis(
        self,
        analyses: List[Dict[str, Any]],
        correlation_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Handle batch credit analysis submissions.
        
        Useful for bulk processing scenarios.
        """
        results = []
        
        for analysis in analyses:
            try:
                await self.handle_credit_analysis_completed(
                    application_id=analysis["application_id"],
                    agent_id=analysis["agent_id"],
                    session_id=analysis["session_id"],
                    risk_tier=analysis["risk_tier"],
                    credit_score=analysis["credit_score"],
                    max_credit_limit=analysis["max_credit_limit"],
                    model_version=analysis["model_version"],
                    confidence_score=analysis.get("confidence_score"),
                    correlation_id=correlation_id
                )
                results.append({
                    "application_id": analysis["application_id"],
                    "status": "success"
                })
            except Exception as e:
                results.append({
                    "application_id": analysis["application_id"],
                    "status": "failed",
                    "error": str(e)
                })
        
        return results
    
    async def handle_batch_compliance_checks(
        self,
        checks: List[Dict[str, Any]],
        correlation_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Handle batch compliance check submissions.
        """
        results = []
        
        for check in checks:
            try:
                await self.handle_compliance_check(
                    application_id=check["application_id"],
                    rule_id=check["rule_id"],
                    regulation_version=check["regulation_version"],
                    check_id=check.get("check_id", f"batch-{uuid.uuid4().hex[:8]}"),
                    passed=check["passed"],
                    details=check.get("details"),
                    failure_reason=check.get("failure_reason"),
                    correlation_id=correlation_id
                )
                results.append({
                    "application_id": check["application_id"],
                    "rule_id": check["rule_id"],
                    "status": "success"
                })
            except Exception as e:
                results.append({
                    "application_id": check["application_id"],
                    "rule_id": check["rule_id"],
                    "status": "failed",
                    "error": str(e)
                })
        
        return results
    
    # =========================================================================
    # Query Helpers (Not part of CQRS, but useful for handlers)
    # =========================================================================
    
    async def get_application_status(self, application_id: str) -> Dict[str, Any]:
        """
        Get current application status for handlers.
        Used by handlers to check state before operations.
        """
        try:
            app = await LoanApplicationAggregate.load(self.store, application_id)
            return {
                "application_id": application_id,
                "state": app.state.value if app.state else None,
                "version": app.version,
                "credit_analysis_tier": app.credit_analysis_tier,
                "fraud_score": app.fraud_score,
                "final_decision": app.final_decision
            }
        except Exception:
            return {
                "application_id": application_id,
                "state": None,
                "version": 0,
                "exists": False
            }
    
    async def get_compliance_status(self, application_id: str) -> Dict[str, Any]:
        """
        Get compliance status for handlers.
        """
        try:
            compliance = await ComplianceRecordAggregate.load(self.store, application_id)
            return compliance.get_compliance_summary()
        except Exception:
            return {
                "application_id": application_id,
                "status": "NOT_INITIATED"
            }
    
    async def get_agent_session_status(self, agent_id: str, session_id: str) -> Dict[str, Any]:
        """
        Get agent session status for handlers.
        """
        try:
            agent = await AgentSessionAggregate.load(self.store, agent_id, session_id)
            return {
                "agent_id": agent_id,
                "session_id": session_id,
                "context_loaded": agent.context_loaded,
                "version": agent.version,
                "actions_taken": len(agent.actions_taken),
                "model_version": agent.model_version
            }
        except Exception:
            return {
                "agent_id": agent_id,
                "session_id": session_id,
                "exists": False
            }