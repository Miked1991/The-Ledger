# src/commands/handlers.py - Complete with all 6 business rules
"""
Complete Command Handlers for The Ledger Event Store.

Implements all 6 business rules with load → validate → determine → append pattern.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid
import logging

from src.event_store import EventStore
from src.aggregates.loan_application import LoanApplicationAggregate
from src.aggregates.agent_session import AgentSessionAggregate
from src.aggregates.compliance_record import ComplianceRecordAggregate, ComplianceStatus
from src.aggregates.audit_ledger import AuditLedgerAggregate
from src.models.events import (
    ApplicationSubmitted, CreditAnalysisCompleted, FraudScreeningCompleted,
    ComplianceRulePassed, ComplianceRuleFailed, DecisionGenerated,
    HumanReviewCompleted, AgentContextLoaded, AgentActionTaken,
    ComplianceCheckInitiated
)
from src.models.errors import (
    DomainError, PreconditionFailedError, InvalidStateTransitionError,
    OptimisticConcurrencyError
)

logger = logging.getLogger(__name__)


class CommandHandlers:
    """
    Complete Command Handlers with all 6 business rules enforced.
    
    Business Rules:
    1. State machine (valid transitions only)
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
        
        Rule #1: State machine initialization
        """
        logger.info(f"Handling submit_application for {application_id}")
        
        # 1. Load aggregate
        try:
            app = await LoanApplicationAggregate.load(self.store, application_id)
            if app.version > 0:
                raise DomainError(f"Application {application_id} already exists")
        except Exception:
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
        
        # 4. Append atomically
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
        
        Rules:
        - #2: Gas Town - Agent must have context loaded
        - #3: Model version locking
        - #1: State machine - must be in AWAITING_ANALYSIS
        """
        logger.info(f"Handling credit_analysis_completed for {application_id}")
        
        # 1. Load aggregates
        app = await LoanApplicationAggregate.load(self.store, application_id)
        agent = await AgentSessionAggregate.load(self.store, agent_id, session_id)
        
        # 2. Validate business rules
        app.assert_awaiting_credit_analysis()  # Rule #1
        app.assert_credit_analysis_not_completed()  # Rule #3 - prevents analysis churn
        agent.assert_context_loaded()  # Rule #2 - Gas Town
        agent.assert_model_version_current(model_version)  # Rule #3 - model locking
        
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
                "model_version": model_version,
                "application_id": application_id
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
        
        Rule #2: Gas Town - Agent must have context loaded
        """
        logger.info(f"Handling fraud_screening_completed for {application_id}")
        
        # 1. Load aggregates
        app = await LoanApplicationAggregate.load(self.store, application_id)
        agent = await AgentSessionAggregate.load(self.store, agent_id, session_id)
        
        # 2. Validate
        if fraud_score < 0.0 or fraud_score > 1.0:
            raise DomainError(f"Fraud score {fraud_score} must be between 0.0 and 1.0")
        agent.assert_context_loaded()  # Rule #2
        
        # 3. Determine events
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
                "risk_indicators": risk_indicators,
                "application_id": application_id
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
        
        logger.info(f"✓ Fraud screening recorded for {application_id}")
    
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
        
        Rule #5: Compliance dependency setup
        """
        logger.info(f"Handling initiate_compliance_checks for {application_id}")
        
        # 1. Load compliance aggregate
        compliance = await ComplianceRecordAggregate.load(self.store, application_id)
        
        # 2. Validate
        compliance.assert_check_not_initiated()
        
        # 3. Determine events
        event = compliance.create_check_initiated_event(
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
        
        Rule #5: Compliance dependency tracking
        """
        logger.info(f"Handling compliance_check for {application_id} - {rule_id}")
        
        # 1. Load compliance aggregate
        compliance = await ComplianceRecordAggregate.load(self.store, application_id)
        
        # 2. Validate
        if compliance.required_checks and rule_id not in compliance.required_checks:
            compliance.assert_rule_exists(rule_id)
        compliance.assert_no_duplicate_check(rule_id)
        
        # 3. Determine events
        if passed:
            event = compliance.create_rule_passed_event(
                rule_id=rule_id,
                regulation_version=regulation_version,
                details=details or {}
            )
        else:
            event = compliance.create_rule_failed_event(
                rule_id=rule_id,
                regulation_version=regulation_version,
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
        
        Rules:
        - #2: Gas Town - Agent must have context loaded
        - #4: Confidence floor: confidence < 0.6 forces REFER
        - #5: Compliance dependency: All required checks must be complete
        - #6: Causal chain: contributing sessions must have processed this application
        """
        logger.info(f"Handling generate_decision for {application_id}")
        
        # 1. Load aggregates
        app = await LoanApplicationAggregate.load(self.store, application_id)
        agent = await AgentSessionAggregate.load(self.store, agent_id, session_id)
        compliance = await ComplianceRecordAggregate.load(self.store, application_id)
        
        # 2. Validate business rules
        app.assert_decision_generation_allowed()  # Rule #1
        agent.assert_context_loaded()  # Rule #2
        
        # Rule #5: Compliance dependency
        if compliance.required_checks:
            compliance.assert_compliance_clearance()
        
        # Rule #6: Causal chain enforcement
        await self._validate_causal_chain(application_id, contributing_sessions)
        
        # 3. Determine events
        decision_event = app.create_decision_event(
            recommendation=orchestrator_recommendation,
            confidence_score=orchestrator_confidence,
            contributing_sessions=contributing_sessions,
            reasoning=reasoning
        )
        app.apply_new_event(decision_event)
        
        # Record agent action
        action_event = agent.create_action_taken_event(
            action_type="generate_decision",
            input_hash=f"hash_{application_id}_{orchestrator_recommendation}",
            reasoning_trace=reasoning,
            output_data={
                "recommendation": orchestrator_recommendation,
                "confidence": orchestrator_confidence,
                "contributing_sessions": contributing_sessions,
                "application_id": application_id,
                "confidence_floor_applied": orchestrator_confidence < 0.6
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
        
        logger.info(f"✓ Decision generated for {application_id}: {orchestrator_recommendation}")
    
    async def _validate_causal_chain(
        self,
        application_id: str,
        contributing_sessions: List[str]
    ) -> None:
        """
        Rule #6: Validate causal chain - all contributing sessions processed this application.
        """
        for session in contributing_sessions:
            # Parse session ID: agent-{agent_id}-{session_id}
            if not session.startswith("agent-"):
                raise DomainError(
                    f"Invalid session ID format: {session}. Expected format: agent-{{agent_id}}-{{session_id}}",
                    session=session,
                    suggested_action="Use valid session ID from start_agent_session"
                )
            
            parts = session.split("-")
            if len(parts) < 3:
                raise DomainError(f"Invalid session ID format: {session}")
            
            contrib_agent_id = parts[1]
            contrib_session_id = "-".join(parts[2:])
            
            # Load the contributing agent session
            try:
                contrib_agent = await AgentSessionAggregate.load(
                    self.store, contrib_agent_id, contrib_session_id
                )
            except Exception:
                raise DomainError(
                    f"Session {session} not found",
                    session=session,
                    suggested_action="Verify session ID exists"
                )
            
            # Check if this session processed this application
            found_application = False
            for action in contrib_agent.actions_taken:
                output = action.get("output_data", {})
                if output.get("application_id") == application_id:
                    found_application = True
                    break
            
            if not found_application:
                raise DomainError(
                    f"Session {session} did not process application {application_id}",
                    session=session,
                    application_id=application_id,
                    actions_taken=[a["action_type"] for a in contrib_agent.actions_taken],
                    suggested_action="Verify contributing_sessions list includes only sessions that actually processed this application"
                )
        
        logger.debug(f"Causal chain validated for {application_id}: {contributing_sessions}")
    
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
        
        Rule #1: Final state transition
        """
        logger.info(f"Handling human_review_completed for {application_id}")
        
        # 1. Load aggregate
        app = await LoanApplicationAggregate.load(self.store, application_id)
        
        # 2. Validate
        app.assert_final_decision_allowed()  # Rule #1
        
        if override and not override_reason:
            raise DomainError("Override requires a reason")
        
        # 3. Determine events
        event = app.create_human_review_event(
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
        
        Rule #2: Gas Town - Agents must load context before making decisions.
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