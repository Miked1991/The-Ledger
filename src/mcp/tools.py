# src/mcp/tools.py
"""
Complete MCP Tools Implementation - Command Side
All 8 tools with precondition documentation and structured errors.
"""

from typing import Dict, Any, Optional, List
import logging
from src.commands.handlers import CommandHandlers
from src.models.errors import PreconditionFailedError, OptimisticConcurrencyError, DomainError
from src.integrity.audit_chain import run_integrity_check

logger = logging.getLogger(__name__)


class MCPTools:
    """
    MCP Tools - Command side of CQRS.
    
    Each tool includes:
    - Precondition documentation in docstring for LLM consumption
    - Structured error types with suggested_action
    - Correlation ID tracing
    - Input validation
    """
    
    def __init__(self, command_handlers: CommandHandlers):
        self.handlers = command_handlers
        self._stats = {
            "total_calls": 0,
            "successful_calls": 0,
            "failed_calls": 0,
            "calls_by_tool": {}
        }
    
    async def execute_tool(self, tool_name: str, arguments: Dict[str, Any], 
                          correlation_id: Optional[str] = None) -> Dict[str, Any]:
        """Execute the requested tool with validation"""
        
        self._stats["total_calls"] += 1
        self._stats["calls_by_tool"][tool_name] = self._stats["calls_by_tool"].get(tool_name, 0) + 1
        
        tool_method = getattr(self, f"tool_{tool_name}", None)
        if not tool_method:
            self._stats["failed_calls"] += 1
            raise ValueError(f"Unknown tool: {tool_name}")
        
        try:
            result = await tool_method(arguments, correlation_id)
            self._stats["successful_calls"] += 1
            return result
        except (PreconditionFailedError, OptimisticConcurrencyError, DomainError) as e:
            self._stats["failed_calls"] += 1
            logger.warning(f"Tool {tool_name} failed: {e}")
            return {
                "success": False,
                "error": e.to_dict(),
                "suggested_next_steps": self._get_suggested_steps(tool_name, e)
            }
        except Exception as e:
            self._stats["failed_calls"] += 1
            logger.error(f"Tool {tool_name} failed with unexpected error: {e}", exc_info=True)
            return {
                "success": False,
                "error": {
                    "type": "InternalError",
                    "message": str(e),
                    "suggested_action": "Contact system administrator"
                }
            }
    
    def _get_suggested_steps(self, tool_name: str, error: Exception) -> List[str]:
        """Get suggested next steps based on error type"""
        if isinstance(error, PreconditionFailedError):
            return ["Check preconditions", error.details.get("suggested_action", "Review documentation")]
        elif isinstance(error, OptimisticConcurrencyError):
            return ["reload_stream_and_retry", "Use exponential backoff"]
        elif isinstance(error, DomainError):
            return ["Review business rules", "Check application state"]
        return ["Contact support"]
    
    async def tool_submit_application(self, args: Dict[str, Any], 
                                      correlation_id: Optional[str]) -> Dict[str, Any]:
        """
        Submit a new loan application.
        
        PRECONDITIONS:
        - Application ID must be unique (not previously used)
        - Application must be in SUBMITTED state (new application)
        - Applicant ID must be valid
        - Requested amount must be > 0
        
        Args:
            application_id: Unique application identifier
            applicant_id: Applicant identifier
            requested_amount: Amount requested in USD
            business_name: Business legal name
            tax_id: Business tax identifier
        
        Returns:
            Dict with stream_id and initial_version
        """
        await self.handlers.handle_submit_application(
            application_id=args["application_id"],
            applicant_id=args["applicant_id"],
            requested_amount=args["requested_amount"],
            business_name=args["business_name"],
            tax_id=args["tax_id"],
            correlation_id=correlation_id
        )
        
        return {
            "success": True,
            "stream_id": f"loan-{args['application_id']}",
            "initial_version": 1,
            "message": f"Application {args['application_id']} submitted"
        }
    
    async def tool_start_agent_session(self, args: Dict[str, Any],
                                       correlation_id: Optional[str]) -> Dict[str, Any]:
        """
        Start a new agent session with context loading (Gas Town pattern).
        
        PRECONDITIONS:
        - Session ID must be unique for this agent
        - This MUST be called before any decision-making tools
        - Context source must be accessible
        - Model version must be valid
        
        Args:
            agent_id: Agent identifier
            session_id: Session identifier
            context_source: Source of context (database, memory, file)
            model_version: Model version being used
            token_count: Number of tokens in context
            context_hash: Hash of context for integrity
        
        Returns:
            Dict with session_id and context_position
        """
        await self.handlers.handle_start_agent_session(
            agent_id=args["agent_id"],
            session_id=args["session_id"],
            context_source=args["context_source"],
            model_version=args["model_version"],
            token_count=args["token_count"],
            context_hash=args["context_hash"],
            correlation_id=correlation_id
        )
        
        return {
            "success": True,
            "session_id": args["session_id"],
            "context_position": 1,
            "message": f"Agent session {args['session_id']} started",
            "next_steps": [
                "Call record_credit_analysis with this session_id",
                "Call record_fraud_screening with this session_id",
                "Call record_compliance_check with this session_id"
            ]
        }
    
    async def tool_record_credit_analysis(self, args: Dict[str, Any],
                                         correlation_id: Optional[str]) -> Dict[str, Any]:
        """
        Record credit analysis results.
        
        PRECONDITIONS:
        - Requires an active agent session created by start_agent_session
        - Application must be in AWAITING_ANALYSIS state
        - Credit analysis not already completed for this application
        - Agent session must have context loaded
        
        Args:
            application_id: Application identifier
            agent_id: Agent identifier
            session_id: Active session identifier
            risk_tier: Risk tier (LOW, MEDIUM, HIGH)
            credit_score: Numeric credit score
            max_credit_limit: Maximum approved credit limit
            model_version: Model version used
            confidence_score: Optional confidence score for the analysis
        
        Returns:
            Dict with event_type and message
        """
        await self.handlers.handle_credit_analysis_completed(
            application_id=args["application_id"],
            agent_id=args["agent_id"],
            session_id=args["session_id"],
            risk_tier=args["risk_tier"],
            credit_score=args["credit_score"],
            max_credit_limit=args["max_credit_limit"],
            model_version=args["model_version"],
            confidence_score=args.get("confidence_score"),
            correlation_id=correlation_id
        )
        
        return {
            "success": True,
            "event_type": "CreditAnalysisCompleted",
            "risk_tier": args["risk_tier"],
            "credit_score": args["credit_score"],
            "message": f"Credit analysis recorded for {args['application_id']}"
        }
    
    async def tool_record_fraud_screening(self, args: Dict[str, Any],
                                         correlation_id: Optional[str]) -> Dict[str, Any]:
        """
        Record fraud screening results.
        
        PRECONDITIONS:
        - Requires an active agent session
        - Fraud score must be between 0.0 and 1.0
        - Application must exist
        
        Args:
            application_id: Application identifier
            agent_id: Agent identifier
            session_id: Active session identifier
            fraud_score: Fraud score (0.0-1.0, higher = more suspicious)
            flags: List of suspicious indicators
            risk_indicators: Additional risk details
        
        Returns:
            Dict with fraud_score and risk_level
        """
        from src.models.events import FraudScreeningCompleted
        
        if args["fraud_score"] < 0.0 or args["fraud_score"] > 1.0:
            raise ValueError(f"Fraud score {args['fraud_score']} must be between 0.0 and 1.0")
        
        event = FraudScreeningCompleted(
            application_id=args["application_id"],
            fraud_score=args["fraud_score"],
            flags=args.get("flags", []),
            risk_indicators=args.get("risk_indicators", {})
        )
        
        stream_id = f"loan-{args['application_id']}"
        version = await self.handlers.store.stream_version(stream_id)
        
        await self.handlers.store.append(
            stream_id,
            [event],
            expected_version=version,
            correlation_id=correlation_id
        )
        
        risk_level = "HIGH" if args["fraud_score"] > 0.7 else "MEDIUM" if args["fraud_score"] > 0.3 else "LOW"
        
        return {
            "success": True,
            "fraud_score": args["fraud_score"],
            "risk_level": risk_level,
            "message": f"Fraud screening recorded for {args['application_id']}"
        }
    
    async def tool_record_compliance_check(self, args: Dict[str, Any],
                                          correlation_id: Optional[str]) -> Dict[str, Any]:
        """
        Record compliance check results.
        
        PRECONDITIONS:
        - Rule ID must exist in active regulation set
        - Compliance check must be initiated
        - Regulation version must be valid
        
        Args:
            application_id: Application identifier
            rule_id: Compliance rule identifier
            regulation_version: Regulation version
            check_id: Unique check identifier
            passed: Whether the check passed
            details: Additional check details
            failure_reason: Reason for failure (if applicable)
        
        Returns:
            Dict with check status
        """
        from src.models.events import ComplianceRulePassed, ComplianceRuleFailed
        
        if args["passed"]:
            event = ComplianceRulePassed(
                application_id=args["application_id"],
                rule_id=args["rule_id"],
                regulation_version=args["regulation_version"],
                check_id=args["check_id"],
                details=args.get("details", {})
            )
        else:
            event = ComplianceRuleFailed(
                application_id=args["application_id"],
                rule_id=args["rule_id"],
                regulation_version=args["regulation_version"],
                check_id=args["check_id"],
                failure_reason=args.get("failure_reason", "Compliance check failed"),
                details=args.get("details", {})
            )
        
        stream_id = f"compliance-{args['application_id']}"
        version = await self.handlers.store.stream_version(stream_id)
        
        await self.handlers.store.append(
            stream_id,
            [event],
            expected_version=version if version > 0 else -1,
            correlation_id=correlation_id
        )
        
        return {
            "success": True,
            "passed": args["passed"],
            "rule_id": args["rule_id"],
            "message": f"Compliance check {args['rule_id']}: {'PASSED' if args['passed'] else 'FAILED'}"
        }
    
    async def tool_generate_decision(self, args: Dict[str, Any],
                                    correlation_id: Optional[str]) -> Dict[str, Any]:
        """
        Generate a decision based on all analyses.
        
        PRECONDITIONS:
        - Requires an active agent session
        - Credit analysis must be complete
        - Fraud screening must be complete
        - Compliance checks must be complete
        - Confidence floor enforcement (confidence < 0.6 forces REFER)
        
        Args:
            application_id: Application identifier
            agent_id: Agent identifier
            session_id: Active session identifier
            orchestrator_recommendation: Recommendation (APPROVE, DECLINE, REFER)
            orchestrator_confidence: Confidence score (0.0-1.0)
            contributing_sessions: List of agent sessions that contributed
            reasoning: Decision reasoning
        
        Returns:
            Dict with decision_id and final_recommendation
        """
        await self.handlers.handle_generate_decision(
            application_id=args["application_id"],
            agent_id=args["agent_id"],
            session_id=args["session_id"],
            orchestrator_recommendation=args["orchestrator_recommendation"],
            orchestrator_confidence=args["orchestrator_confidence"],
            contributing_sessions=args["contributing_sessions"],
            reasoning=args["reasoning"],
            correlation_id=correlation_id
        )
        
        final_recommendation = args["orchestrator_recommendation"]
        confidence_floor_applied = False
        
        if args["orchestrator_confidence"] < 0.6:
            final_recommendation = "REFER"
            confidence_floor_applied = True
        
        return {
            "success": True,
            "decision_id": f"dec-{args['application_id']}",
            "final_recommendation": final_recommendation,
            "confidence_floor_applied": confidence_floor_applied,
            "message": f"Decision generated for {args['application_id']}"
        }
    
    async def tool_record_human_review(self, args: Dict[str, Any],
                                      correlation_id: Optional[str]) -> Dict[str, Any]:
        """
        Record human review and final decision.
        
        PRECONDITIONS:
        - Application must be in PENDING_HUMAN state
        - Reviewer must be authenticated
        - Override requires reason if override=True
        
        Args:
            application_id: Application identifier
            reviewer_id: Reviewer identifier
            final_decision: Final decision (APPROVED, DECLINED)
            override: Whether this overrides AI recommendation
            override_reason: Reason for override if applicable
            comments: Optional review comments
        
        Returns:
            Dict with final_decision and application_state
        """
        await self.handlers.handle_human_review_completed(
            application_id=args["application_id"],
            reviewer_id=args["reviewer_id"],
            final_decision=args["final_decision"],
            override=args.get("override", False),
            override_reason=args.get("override_reason"),
            comments=args.get("comments"),
            correlation_id=correlation_id
        )
        
        return {
            "success": True,
            "final_decision": args["final_decision"],
            "application_state": "FINAL_APPROVED" if args["final_decision"] == "APPROVED" else "FINAL_DECLINED",
            "override": args.get("override", False),
            "message": f"Human review recorded for {args['application_id']}"
        }
    
    async def tool_run_integrity_check(self, args: Dict[str, Any],
                                      correlation_id: Optional[str]) -> Dict[str, Any]:
        """
        Run cryptographic integrity check on audit trail.
        
        PRECONDITIONS:
        - Must be called by compliance role
        - Rate-limited to 1/minute per entity
        - Entity must exist
        
        Args:
            entity_type: Type of entity (loan, agent, compliance)
            entity_id: Entity identifier
        
        Returns:
            Dict with check_result including chain_valid and tamper_detected
        """
        result = await run_integrity_check(
            self.handlers.store,
            args["entity_type"],
            args["entity_id"]
        )
        
        return {
            "success": True,
            "result": result.to_dict(),
            "recommendation": "Chain verified - no issues detected" if result.chain_valid else "Tampering detected - investigate immediately",
            "message": f"Integrity check completed for {args['entity_type']}-{args['entity_id']}"
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get tool statistics"""
        return {
            **self._stats,
            "success_rate": (self._stats["successful_calls"] / self._stats["total_calls"] * 100) 
                            if self._stats["total_calls"] > 0 else 0
        }