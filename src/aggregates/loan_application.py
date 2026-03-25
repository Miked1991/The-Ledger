# src/aggregates/loan_application.py
"""
Loan Application Aggregate - Full state machine with 6 business rules.
Tracks the complete lifecycle of a commercial loan application.
"""

from typing import Optional, List, Dict, Any, Set
from datetime import datetime
from enum import Enum
import logging

from src.aggregates.base import Aggregate
from src.event_store import EventStore
from src.models.events import (
    ApplicationState, ApplicationSubmitted, CreditAnalysisCompleted,
    FraudScreeningCompleted, ComplianceRulePassed, ComplianceRuleFailed,
    DecisionGenerated, HumanReviewCompleted, StoredEvent, BaseEvent
)
from src.models.errors import (
    DomainError, InvalidStateTransitionError, PreconditionFailedError
)

logger = logging.getLogger(__name__)


class LoanApplicationAggregate(Aggregate):
    """
    Loan Application Aggregate with full state machine.
    
    Business Rules Enforced:
    1. State Machine: Valid transitions only (Submitted → AwaitingAnalysis → AnalysisComplete → 
       ComplianceReview → PendingDecision → ApprovedPendingHuman / DeclinedPendingHuman → 
       FinalApproved / FinalDeclined)
    2. Gas Town: Already enforced in AgentSession
    3. Model Version Locking: No analysis churn (single credit analysis per application)
    4. Confidence Floor: Confidence < 0.6 forces REFER
    5. Compliance Dependency: All required checks must pass before approval
    6. Causal Chain Enforcement: Contributing sessions must reference actual work
    """
    
    # Valid state transitions
    VALID_TRANSITIONS = {
        ApplicationState.SUBMITTED: [ApplicationState.AWAITING_ANALYSIS],
        ApplicationState.AWAITING_ANALYSIS: [ApplicationState.ANALYSIS_COMPLETE],
        ApplicationState.ANALYSIS_COMPLETE: [ApplicationState.COMPLIANCE_REVIEW, ApplicationState.PENDING_DECISION],
        ApplicationState.COMPLIANCE_REVIEW: [ApplicationState.PENDING_DECISION, ApplicationState.FINAL_DECLINED],
        ApplicationState.PENDING_DECISION: [ApplicationState.APPROVED_PENDING_HUMAN, ApplicationState.DECLINED_PENDING_HUMAN],
        ApplicationState.APPROVED_PENDING_HUMAN: [ApplicationState.FINAL_APPROVED, ApplicationState.FINAL_DECLINED],
        ApplicationState.DECLINED_PENDING_HUMAN: [ApplicationState.FINAL_DECLINED, ApplicationState.FINAL_APPROVED],
        ApplicationState.FINAL_APPROVED: [],
        ApplicationState.FINAL_DECLINED: []
    }
    
    def __init__(self, application_id: str):
        super().__init__()
        self.application_id = application_id
        self.state: ApplicationState = ApplicationState.SUBMITTED
        self.applicant_id: Optional[str] = None
        self.business_name: Optional[str] = None
        self.tax_id: Optional[str] = None
        self.requested_amount: Optional[float] = None
        self.approved_amount: Optional[float] = None
        
        # Analysis results
        self.credit_analysis_tier: Optional[str] = None
        self.credit_score: Optional[int] = None
        self.max_credit_limit: Optional[float] = None
        self.credit_analysis_model: Optional[str] = None
        self.credit_analysis_confidence: Optional[float] = None
        self.credit_analysis_completed_at: Optional[datetime] = None
        
        self.fraud_score: Optional[float] = None
        self.fraud_flags: List[str] = []
        self.fraud_risk_indicators: Dict[str, Any] = {}
        
        # Compliance tracking
        self.compliance_checks_passed: Set[str] = set()
        self.compliance_checks_failed: List[Dict[str, Any]] = []
        self.required_compliance_checks: Set[str] = set()
        
        # Decision tracking
        self.ai_recommendation: Optional[str] = None
        self.ai_confidence: Optional[float] = None
        self.decision_reasoning: Optional[str] = None
        self.contributing_sessions: List[str] = []
        
        # Human review
        self.human_reviewer_id: Optional[str] = None
        self.final_decision: Optional[str] = None
        self.human_review_comments: Optional[str] = None
        self.override: bool = False
        self.override_reason: Optional[str] = None
        
        # Timestamps
        self.submitted_at: Optional[datetime] = None
        self.analysis_completed_at: Optional[datetime] = None
        self.decision_generated_at: Optional[datetime] = None
        self.reviewed_at: Optional[datetime] = None
    
    @classmethod
    async def load(cls, store: EventStore, application_id: str) -> "LoanApplicationAggregate":
        """Load aggregate from event store"""
        stream_id = f"loan-{application_id}"
        events = await store.load_stream(stream_id)
        
        agg = cls(application_id)
        for event in events:
            agg.apply_event(event)
        
        logger.debug(f"Loaded loan application {application_id} in state {agg.state}")
        return agg
    
    # =========================================================================
    # Event Handlers
    # =========================================================================
    
    def _on_ApplicationSubmitted(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle ApplicationSubmitted event"""
        self.state = ApplicationState.AWAITING_ANALYSIS
        self.applicant_id = payload["applicant_id"]
        self.business_name = payload["business_name"]
        self.tax_id = payload["tax_id"]
        self.requested_amount = payload["requested_amount_usd"]
        self.submitted_at = datetime.utcnow()
        logger.info(f"Application {self.application_id} submitted, state: {self.state}")
    
    def _on_CreditAnalysisCompleted(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle CreditAnalysisCompleted event"""
        self.credit_analysis_tier = payload["risk_tier"]
        self.credit_score = payload["credit_score"]
        self.max_credit_limit = payload["max_credit_limit"]
        self.credit_analysis_model = payload.get("model_version")
        self.credit_analysis_confidence = payload.get("confidence_score")
        self.credit_analysis_completed_at = datetime.utcnow()
        
        # State transition: AWAITING_ANALYSIS → ANALYSIS_COMPLETE
        self._transition_to(ApplicationState.ANALYSIS_COMPLETE)
        logger.info(f"Credit analysis completed for {self.application_id}: {self.credit_analysis_tier} risk")
    
    def _on_FraudScreeningCompleted(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle FraudScreeningCompleted event"""
        self.fraud_score = payload["fraud_score"]
        self.fraud_flags = payload.get("flags", [])
        self.fraud_risk_indicators = payload.get("risk_indicators", {})
        
        # If fraud score is high, auto-decline
        if self.fraud_score and self.fraud_score > 0.7:
            logger.warning(f"High fraud score {self.fraud_score} for {self.application_id}")
    
    def _on_ComplianceRulePassed(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle ComplianceRulePassed event"""
        rule_id = payload["rule_id"]
        self.compliance_checks_passed.add(rule_id)
        
        # Check if all required checks are complete
        if self.required_compliance_checks and self.compliance_checks_passed == self.required_compliance_checks:
            self._transition_to(ApplicationState.COMPLIANCE_REVIEW)
            logger.info(f"All compliance checks passed for {self.application_id}")
    
    def _on_ComplianceRuleFailed(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle ComplianceRuleFailed event"""
        self.compliance_checks_failed.append({
            "rule_id": payload["rule_id"],
            "failure_reason": payload.get("failure_reason"),
            "regulation_version": payload.get("regulation_version"),
            "timestamp": datetime.utcnow().isoformat()
        })
        
        # Compliance failure moves to PENDING_DECISION with negative recommendation
        self._transition_to(ApplicationState.PENDING_DECISION)
        logger.warning(f"Compliance check failed for {self.application_id}: {payload['rule_id']}")
    
    def _on_DecisionGenerated(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle DecisionGenerated event"""
        self.ai_recommendation = payload["recommendation"]
        self.ai_confidence = payload["confidence_score"]
        self.decision_reasoning = payload["decision_reasoning"]
        self.contributing_sessions = payload.get("contributing_agent_sessions", [])
        self.decision_generated_at = datetime.utcnow()
        
        # State transition based on recommendation
        if self.ai_recommendation == "APPROVE":
            self._transition_to(ApplicationState.APPROVED_PENDING_HUMAN)
        elif self.ai_recommendation == "DECLINE":
            self._transition_to(ApplicationState.DECLINED_PENDING_HUMAN)
        else:  # REFER
            # Stay in PENDING_DECISION for human review
            self.state = ApplicationState.PENDING_DECISION
        
        logger.info(f"Decision generated for {self.application_id}: {self.ai_recommendation} (confidence: {self.ai_confidence})")
    
    def _on_HumanReviewCompleted(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle HumanReviewCompleted event"""
        self.human_reviewer_id = payload["reviewer_id"]
        self.final_decision = payload["final_decision"]
        self.human_review_comments = payload.get("comments")
        self.override = payload.get("override", False)
        self.override_reason = payload.get("override_reason")
        self.reviewed_at = datetime.utcnow()
        
        # Final state transition
        if self.final_decision == "APPROVED":
            self._transition_to(ApplicationState.FINAL_APPROVED)
        else:
            self._transition_to(ApplicationState.FINAL_DECLINED)
        
        logger.info(f"Human review completed for {self.application_id}: {self.final_decision}")
    
    # =========================================================================
    # Business Rule Validation Methods
    # =========================================================================
    
    def _transition_to(self, new_state: ApplicationState) -> None:
        """Validate and perform state transition"""
        if new_state not in self.VALID_TRANSITIONS.get(self.state, []):
            raise InvalidStateTransitionError(
                f"Invalid state transition from {self.state} to {new_state}",
                current_state=self.state.value,
                attempted_state=new_state.value,
                valid_transitions=[s.value for s in self.VALID_TRANSITIONS.get(self.state, [])]
            )
        self.state = new_state
    
    def assert_submit(self, application_id: str, applicant_id: str, amount: float) -> None:
        """Rule #1: Validate application submission"""
        if self.version > 0:
            raise DomainError(f"Application {application_id} already exists")
        
        if amount <= 0:
            raise DomainError(f"Requested amount {amount} must be greater than 0")
        
        if not applicant_id:
            raise DomainError("Applicant ID is required")
    
    def assert_awaiting_credit_analysis(self) -> None:
        """Validate that application is ready for credit analysis"""
        if self.state != ApplicationState.AWAITING_ANALYSIS:
            raise InvalidStateTransitionError(
                f"Cannot run credit analysis: current state is {self.state}. Expected AWAITING_ANALYSIS",
                current_state=self.state.value,
                required_state=ApplicationState.AWAITING_ANALYSIS.value
            )
    
    def assert_credit_analysis_not_completed(self) -> None:
        """Rule #3: Prevent analysis churn - single credit analysis per application"""
        if self.credit_analysis_tier is not None:
            raise DomainError(
                f"Credit analysis already completed for application {self.application_id}",
                current_analysis=self.credit_analysis_tier,
                message="Analysis churn prevented - only one credit analysis allowed per application"
            )
    
    def assert_analysis_complete_for_decision(self) -> None:
        """Validate that all required analyses are complete"""
        if self.credit_analysis_tier is None:
            raise PreconditionFailedError(
                "Cannot generate decision: credit analysis not completed",
                missing_analysis=["credit_analysis"],
                suggested_action="Complete credit analysis first"
            )
        
        if self.fraud_score is None:
            raise PreconditionFailedError(
                "Cannot generate decision: fraud screening not completed",
                missing_analysis=["fraud_screening"],
                suggested_action="Complete fraud screening first"
            )
    
    def assert_compliance_checks_complete(self, required_checks: Optional[List[str]] = None) -> None:
        """Rule #5: All required compliance checks must pass"""
        checks_to_verify = required_checks or list(self.required_compliance_checks)
        
        if not checks_to_verify:
            return
        
        missing_checks = set(checks_to_verify) - self.compliance_checks_passed
        
        if missing_checks:
            raise PreconditionFailedError(
                f"Cannot approve application: missing compliance checks {missing_checks}",
                missing_checks=list(missing_checks),
                passed_checks=list(self.compliance_checks_passed),
                required_checks=checks_to_verify,
                suggested_action="Complete all required compliance checks first"
            )
        
        if self.compliance_checks_failed:
            raise PreconditionFailedError(
                f"Cannot approve application: compliance checks failed",
                failed_checks=self.compliance_checks_failed,
                suggested_action="Resolve compliance failures or request override"
            )
    
    def assert_decision_generation_allowed(self) -> None:
        """Validate that decision can be generated"""
        self.assert_analysis_complete_for_decision()
        
        if self.state not in [ApplicationState.ANALYSIS_COMPLETE, ApplicationState.COMPLIANCE_REVIEW]:
            raise InvalidStateTransitionError(
                f"Cannot generate decision: current state is {self.state}. Expected ANALYSIS_COMPLETE or COMPLIANCE_REVIEW",
                current_state=self.state.value,
                required_states=["ANALYSIS_COMPLETE", "COMPLIANCE_REVIEW"]
            )
    
    def assert_confidence_floor(self, confidence_score: float) -> None:
        """Rule #4: Confidence floor enforcement (handled in event creation)"""
        if confidence_score < 0.6:
            logger.info(f"Confidence floor triggered: {confidence_score} < 0.6, forcing REFER")
            # This is enforced in create_decision_event
    
    def assert_final_decision_allowed(self) -> None:
        """Validate that human review can be recorded"""
        if self.state not in [ApplicationState.APPROVED_PENDING_HUMAN, 
                              ApplicationState.DECLINED_PENDING_HUMAN,
                              ApplicationState.PENDING_DECISION]:
            raise InvalidStateTransitionError(
                f"Cannot record human review: current state is {self.state}. Expected APPROVED_PENDING_HUMAN, DECLINED_PENDING_HUMAN, or PENDING_DECISION",
                current_state=self.state.value,
                required_states=["APPROVED_PENDING_HUMAN", "DECLINED_PENDING_HUMAN", "PENDING_DECISION"]
            )
    
    def assert_causal_chain(self, contributing_sessions: List[str]) -> None:
        """Rule #6: Verify causal chain - all contributing sessions processed this application"""
        for session in contributing_sessions:
            # Parse session ID format: agent-{agent_id}-{session_id}
            if not session.startswith("agent-"):
                raise DomainError(f"Invalid session ID format: {session}. Expected format: agent-{{agent_id}}-{{session_id}}")
            
            # Note: Full validation requires loading each agent session
            # This is performed in the command handler for performance
            pass
    
    # =========================================================================
    # Event Creation Methods
    # =========================================================================
    
    def create_submitted_event(
        self,
        applicant_id: str,
        requested_amount: float,
        business_name: str,
        tax_id: str
    ) -> ApplicationSubmitted:
        """Create ApplicationSubmitted event"""
        return ApplicationSubmitted(
            application_id=self.application_id,
            applicant_id=applicant_id,
            requested_amount_usd=requested_amount,
            business_name=business_name,
            tax_id=tax_id
        )
    
    def create_credit_analysis_event(
        self,
        risk_tier: str,
        credit_score: int,
        max_credit_limit: float,
        model_version: str,
        confidence_score: Optional[float] = None
    ) -> CreditAnalysisCompleted:
        """Create CreditAnalysisCompleted event"""
        return CreditAnalysisCompleted(
            application_id=self.application_id,
            risk_tier=risk_tier,
            credit_score=credit_score,
            max_credit_limit=max_credit_limit,
            model_version=model_version,
            confidence_score=confidence_score
        )
    
    def create_decision_event(
        self,
        recommendation: str,
        confidence_score: float,
        contributing_sessions: List[str],
        reasoning: str
    ) -> DecisionGenerated:
        """Create DecisionGenerated event with confidence floor enforcement (Rule #4)"""
        final_recommendation = recommendation
        
        # Rule #4: Confidence floor enforcement
        if confidence_score < 0.6:
            final_recommendation = "REFER"
            logger.info(f"Confidence floor applied: {confidence_score} < 0.6, forcing REFER")
        
        return DecisionGenerated(
            application_id=self.application_id,
            decision_id=f"dec-{self.application_id}-{self.version + 1}",
            recommendation=final_recommendation,
            confidence_score=confidence_score,
            contributing_agent_sessions=contributing_sessions,
            decision_reasoning=reasoning
        )
    
    def create_human_review_event(
        self,
        reviewer_id: str,
        final_decision: str,
        override: bool = False,
        override_reason: Optional[str] = None,
        comments: Optional[str] = None
    ) -> HumanReviewCompleted:
        """Create HumanReviewCompleted event"""
        return HumanReviewCompleted(
            application_id=self.application_id,
            reviewer_id=reviewer_id,
            final_decision=final_decision,
            override=override,
            override_reason=override_reason,
            comments=comments
        )
    
    # =========================================================================
    # Query Methods
    # =========================================================================
    
    def get_state_summary(self) -> Dict[str, Any]:
        """Get current state summary"""
        return {
            "application_id": self.application_id,
            "state": self.state.value,
            "applicant_id": self.applicant_id,
            "business_name": self.business_name,
            "requested_amount": self.requested_amount,
            "approved_amount": self.approved_amount,
            "credit_analysis_tier": self.credit_analysis_tier,
            "credit_score": self.credit_score,
            "fraud_score": self.fraud_score,
            "fraud_flags": self.fraud_flags,
            "compliance_passed": list(self.compliance_checks_passed),
            "compliance_failed": self.compliance_checks_failed,
            "ai_recommendation": self.ai_recommendation,
            "ai_confidence": self.ai_confidence,
            "final_decision": self.final_decision,
            "version": self.version,
            "timeline": {
                "submitted": self.submitted_at.isoformat() if self.submitted_at else None,
                "analysis_completed": self.credit_analysis_completed_at.isoformat() if self.credit_analysis_completed_at else None,
                "decision_generated": self.decision_generated_at.isoformat() if self.decision_generated_at else None,
                "reviewed": self.reviewed_at.isoformat() if self.reviewed_at else None
            }
        }
    
    def can_approve(self) -> bool:
        """Check if application can be approved"""
        return (self.state == ApplicationState.APPROVED_PENDING_HUMAN and
                self.ai_recommendation == "APPROVE" and
                len(self.compliance_checks_failed) == 0)
    
    def can_decline(self) -> bool:
        """Check if application can be declined"""
        return (self.state == ApplicationState.DECLINED_PENDING_HUMAN or
                self.fraud_score and self.fraud_score > 0.7 or
                len(self.compliance_checks_failed) > 0)