# src/mcp/tools.py
"""
MCP tools for command side operations.
Each tool implements the command side of CQRS.
"""

from typing import Dict, Any, Optional
import logging
from pydantic import BaseModel, Field

from ..event_store import EventStore
from ..commands.handlers import (
    SubmitApplicationCommand,
    CreditAnalysisCompletedCommand,
    StartAgentSessionCommand,
    GenerateDecisionCommand,
    handle_submit_application,
    handle_credit_analysis_completed,
    handle_start_agent_session,
    handle_generate_decision,
)
from ..models.errors import OptimisticConcurrencyError, DomainError, PreconditionFailedError

logger = logging.getLogger(__name__)


class ToolResult(BaseModel):
    """Structured result from tool execution."""
    success: bool
    result: Optional[Dict[str, Any]] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    suggested_action: Optional[str] = None
    stream_id: Optional[str] = None
    expected_version: Optional[int] = None
    actual_version: Optional[int] = None


class MCPTools:
    """
    MCP Tools for The Ledger.
    
    These tools are designed to be consumed by LLM agents.
    Each tool has:
    - Clear precondition documentation in description
    - Structured error types with suggested actions
    - Pydantic validation for parameters
    """
    
    def __init__(self, store: EventStore):
        self.store = store
    
    async def submit_application(
        self,
        application_id: str,
        applicant_id: str,
        requested_amount_usd: float,
        applicant_name: str,
        applicant_tin: str,
        business_type: str,
        annual_revenue: Optional[float] = None
    ) -> ToolResult:
        """
        Submit a new loan application.
        
        Preconditions:
        - application_id must be unique (not previously submitted)
        - applicant_id must exist in the system
        - requested_amount_usd must be positive
        
        Returns:
            ToolResult with success status and result details
        """
        try:
            cmd = SubmitApplicationCommand(
                application_id=application_id,
                applicant_id=applicant_id,
                requested_amount_usd=requested_amount_usd,
                applicant_name=applicant_name,
                applicant_tin=applicant_tin,
                business_type=business_type,
                annual_revenue=annual_revenue
            )
            stream_id = await handle_submit_application(cmd, self.store)
            
            return ToolResult(
                success=True,
                result={
                    "stream_id": stream_id,
                    "application_id": application_id,
                    "message": "Application submitted successfully"
                }
            )
        except DomainError as e:
            return ToolResult(
                success=False,
                error_type="DomainError",
                error_message=str(e),
                suggested_action="Check application ID uniqueness and input data"
            )
        except Exception as e:
            logger.error(f"Unexpected error in submit_application: {e}")
            return ToolResult(
                success=False,
                error_type="InternalError",
                error_message=str(e),
                suggested_action="Contact system administrator"
            )
    
    async def start_agent_session(
        self,
        agent_id: str,
        session_id: str,
        context_source: str,
        model_version: str,
        available_actions: list
    ) -> ToolResult:
        """
        Start an AI agent session with context loaded.
        
        Preconditions:
        - session_id must be unique (not already started)
        - model_version must be a valid deployed version
        - context_source must contain the agent's context data
        
        This tool implements the Gas Town pattern - it loads the agent's
        context into memory that will be persisted in the event store,
        allowing the agent to recover after crashes.
        """
        try:
            cmd = StartAgentSessionCommand(
                agent_id=agent_id,
                session_id=session_id,
                context_source=context_source,
                model_version=model_version,
                available_actions=available_actions
            )
            stream_id = await handle_start_agent_session(cmd, self.store)
            
            return ToolResult(
                success=True,
                result={
                    "stream_id": stream_id,
                    "agent_id": agent_id,
                    "session_id": session_id,
                    "message": "Agent session started with context loaded"
                }
            )
        except DomainError as e:
            return ToolResult(
                success=False,
                error_type="DomainError",
                error_message=str(e),
                suggested_action="Use a unique session_id or check model_version validity"
            )
        except Exception as e:
            logger.error(f"Unexpected error in start_agent_session: {e}")
            return ToolResult(
                success=False,
                error_type="InternalError",
                error_message=str(e)
            )
    
    async def record_credit_analysis(
        self,
        application_id: str,
        agent_id: str,
        session_id: str,
        credit_score: int,
        risk_tier: str,
        debt_to_income_ratio: float,
        model_version: str,
        confidence_score: Optional[float] = None
    ) -> ToolResult:
        """
        Record completed credit analysis.
        
        Preconditions:
        - Active agent session must exist with context loaded
        - Application must be in Submitted or AwaitingAnalysis state
        - No previous credit analysis for this application (unless overridden)
        """
        try:
            cmd = CreditAnalysisCompletedCommand(
                application_id=application_id,
                agent_id=agent_id,
                session_id=session_id,
                credit_score=credit_score,
                risk_tier=risk_tier,
                debt_to_income_ratio=debt_to_income_ratio,
                model_version=model_version,
                confidence_score=confidence_score
            )
            stream_id = await handle_credit_analysis_completed(cmd, self.store)
            
            return ToolResult(
                success=True,
                result={
                    "stream_id": stream_id,
                    "application_id": application_id,
                    "message": "Credit analysis recorded"
                }
            )
        except OptimisticConcurrencyError as e:
            return ToolResult(
                success=False,
                error_type="OptimisticConcurrencyError",
                error_message=str(e),
                suggested_action=e.suggested_action if hasattr(e, 'suggested_action') else "Reload application and retry",
                stream_id=e.stream_id,
                expected_version=e.expected_version,
                actual_version=e.actual_version
            )
        except DomainError as e:
            return ToolResult(
                success=False,
                error_type="DomainError",
                error_message=str(e),
                suggested_action="Check application state and agent session validity"
            )
        except PreconditionFailedError as e:
            return ToolResult(
                success=False,
                error_type="PreconditionFailed",
                error_message=str(e),
                suggested_action=e.details.get("suggested_action", "Start agent session first")
            )
        except Exception as e:
            logger.error(f"Unexpected error in record_credit_analysis: {e}")
            return ToolResult(
                success=False,
                error_type="InternalError",
                error_message=str(e)
            )
    
    async def generate_decision(
        self,
        application_id: str,
        recommendation: str,
        confidence_score: float,
        contributing_agent_sessions: list,
        rationale: str
    ) -> ToolResult:
        """
        Generate a decision recommendation.
        
        Preconditions:
        - Credit analysis and fraud screening must be completed
        - All compliance checks must be passed
        - No decision already generated
        - If confidence_score < 0.6, recommendation must be "REFER"
        """
        try:
            cmd = GenerateDecisionCommand(
                application_id=application_id,
                recommendation=recommendation,
                confidence_score=confidence_score,
                contributing_agent_sessions=contributing_agent_sessions,
                rationale=rationale
            )
            stream_id = await handle_generate_decision(cmd, self.store)
            
            return ToolResult(
                success=True,
                result={
                    "stream_id": stream_id,
                    "application_id": application_id,
                    "recommendation": recommendation,
                    "message": "Decision generated"
                }
            )
        except OptimisticConcurrencyError as e:
            return ToolResult(
                success=False,
                error_type="OptimisticConcurrencyError",
                error_message=str(e),
                suggested_action="Reload application and retry"
            )
        except DomainError as e:
            return ToolResult(
                success=False,
                error_type="DomainError",
                error_message=str(e),
                suggested_action="Check confidence floor and required analyses"
            )
        except Exception as e:
            logger.error(f"Unexpected error in generate_decision: {e}")
            return ToolResult(
                success=False,
                error_type="InternalError",
                error_message=str(e)
            )