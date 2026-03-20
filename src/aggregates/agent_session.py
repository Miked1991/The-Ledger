"""
ledger/aggregates/agent_session.py — Agent Session Aggregate
=============================================================
Implements Gas Town persistent ledger pattern for AI agent sessions.

Stream format: "agent-{agent_type}-{session_id}"

Key features:
- Gas Town context enforcement (no decisions without context)
- Model version tracking and locking
- Session health monitoring
- Crash recovery support
- Token budget tracking
"""

from __future__ import annotations
from datetime import datetime
from typing import List, Dict, Any, Optional, Set
from enum import Enum

# Import from canonical event schema
from ledger.schema.events import (
    BaseEvent,
    AgentSessionStarted,
    AgentInputValidated,
    AgentInputValidationFailed,
    AgentNodeExecuted,
    AgentToolCalled,
    AgentOutputWritten,
    AgentSessionCompleted,
    AgentSessionFailed,
    AgentSessionRecovered,
    AgentType,
    CreditAnalysisCompleted,
    DecisionGenerated,
    FraudScreeningCompleted,
    ComplianceCheckCompleted
)


class SessionHealthStatus(str, Enum):
    """Health status for agent sessions"""
    HEALTHY = "HEALTHY"
    HEALTHY_IDLE = "HEALTHY_IDLE"
    HAS_PENDING_WORK = "HAS_PENDING_WORK"
    NEEDS_RECONCILIATION = "NEEDS_RECONCILIATION"
    FAILED = "FAILED"
    RECOVERED = "RECOVERED"
    COMPLETED = "COMPLETED"


class SessionState(str, Enum):
    """State machine for agent sessions"""
    INITIALIZED = "INITIALIZED"
    VALIDATING_INPUT = "VALIDATING_INPUT"
    EXECUTING = "EXECUTING"
    CALLING_TOOLS = "CALLING_TOOLS"
    WRITING_OUTPUT = "WRITING_OUTPUT"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RECOVERING = "RECOVERING"


class AgentSessionAggregate:
    """
    Agent session aggregate implementing Gas Town persistent ledger pattern.
    
    Every agent action must be preceded by a context-loaded event (AgentSessionStarted).
    On crash, session can be reconstructed from event stream and continue where left off.
    
    Business Rules:
    1. Gas Town: No decision events without AgentSessionStarted first
    2. Model Version Locking: All decisions must use session's loaded model version
    3. Causal Chain: Every output must reference its input validation
    4. Session Health: Track pending work for crash recovery
    """
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.agent_type: Optional[AgentType] = None
        self.agent_id: Optional[str] = None
        self.application_id: Optional[str] = None
        
        # Gas Town: context must be loaded first
        self.context_loaded: bool = False
        self.context_source: Optional[str] = None
        self.context_token_count: int = 0
        
        # Model version tracking
        self.model_version: Optional[str] = None
        self.langgraph_graph_version: Optional[str] = None
        
        # Session state
        self.state: SessionState = SessionState.INITIALIZED
        self.health: SessionHealthStatus = SessionHealthStatus.HEALTHY
        self.started_at: Optional[datetime] = None
        self.last_heartbeat: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.failed_at: Optional[datetime] = None
        
        # Execution tracking
        self.nodes_executed: List[Dict[str, Any]] = []
        self.tools_called: List[Dict[str, Any]] = []
        self.events_written: List[Dict[str, Any]] = []
        
        # Token and cost tracking
        self.total_llm_calls: int = 0
        self.total_tokens_input: int = 0
        self.total_tokens_output: int = 0
        self.total_cost_usd: float = 0.0
        self.total_duration_ms: int = 0
        
        # Applications this session has processed (for causal chains)
        self.processed_applications: Set[str] = set()
        
        # Pending work tracking (for crash recovery)
        self.pending_nodes: List[Dict[str, Any]] = []
        self.pending_tool_calls: List[Dict[str, Any]] = []
        self.last_successful_node: Optional[str] = None
        
        # Error tracking
        self.error_type: Optional[str] = None
        self.error_message: Optional[str] = None
        self.recoverable: bool = True
        
        # Recovery tracking
        self.recovered_from_session_id: Optional[str] = None
        self.recovery_point: Optional[str] = None
        
        # Stream version
        self.version: int = 0
    
    @classmethod
    async def load(cls, store, session_id: str) -> "AgentSessionAggregate":
        """
        Load aggregate by replaying event stream.
        
        Args:
            store: EventStore instance
            session_id: Session ID (full stream ID will be constructed)
            
        Returns:
            Reconstructed AgentSessionAggregate
        """
        # Stream ID format: "agent-{agent_type}-{session_id}"
        # Since we don't know agent_type yet, we need to search
        # In practice, callers will know the full stream_id
        stream_id = f"agent-{session_id}"  # Simplified - real impl would need agent_type
        
        events = await store.load_stream(stream_id)
        agg = cls(session_id)
        
        for event in events:
            agg.apply(event)
            
        return agg
    
    @classmethod
    async def load_from_stream(cls, store, stream_id: str) -> "AgentSessionAggregate":
        """
        Load aggregate from full stream ID.
        
        Args:
            store: EventStore instance
            stream_id: Full stream ID (e.g., "agent-credit_analysis-sess-123")
        """
        events = await store.load_stream(stream_id)
        
        # Extract session_id from stream_id
        # Format: "agent-{agent_type}-{session_id}"
        parts = stream_id.split("-")
        if len(parts) < 3:
            raise ValueError(f"Invalid stream_id format: {stream_id}")
        session_id = "-".join(parts[2:])  # Everything after agent-{type}-
        
        agg = cls(session_id)
        for event in events:
            agg.apply(event)
            
        return agg
    
    def apply(self, event: BaseEvent) -> None:
        """
        Apply event to update aggregate state.
        
        Args:
            event: Domain event (from canonical schema)
        """
        # Dispatch to appropriate handler based on event type
        event_type = event.event_type
        
        if event_type == "AgentSessionStarted":
            self._on_session_started(event)
        elif event_type == "AgentInputValidated":
            self._on_input_validated(event)
        elif event_type == "AgentInputValidationFailed":
            self._on_input_validation_failed(event)
        elif event_type == "AgentNodeExecuted":
            self._on_node_executed(event)
        elif event_type == "AgentToolCalled":
            self._on_tool_called(event)
        elif event_type == "AgentOutputWritten":
            self._on_output_written(event)
        elif event_type == "AgentSessionCompleted":
            self._on_session_completed(event)
        elif event_type == "AgentSessionFailed":
            self._on_session_failed(event)
        elif event_type == "AgentSessionRecovered":
            self._on_session_recovered(event)
        
        # Also handle domain events that this agent might have produced
        # These are tracked for causal chain enforcement
        elif event_type == "CreditAnalysisCompleted":
            self._on_credit_analysis_completed(event)
        elif event_type == "DecisionGenerated":
            self._on_decision_generated(event)
        elif event_type == "FraudScreeningCompleted":
            self._on_fraud_screening_completed(event)
        elif event_type == "ComplianceCheckCompleted":
            self._on_compliance_check_completed(event)
        
        # Update version
        self.version += 1
        self.last_heartbeat = datetime.utcnow()
    
    def _on_session_started(self, event: AgentSessionStarted) -> None:
        """Gas Town: First event - context loaded"""
        self.agent_type = event.agent_type
        self.agent_id = event.agent_id
        self.application_id = event.application_id
        self.model_version = event.model_version
        self.langgraph_graph_version = event.langgraph_graph_version
        self.context_source = event.context_source
        self.context_token_count = event.context_token_count
        self.started_at = event.started_at
        
        # Gas Town: context is now loaded
        self.context_loaded = True
        self.state = SessionState.INITIALIZED
        self.health = SessionHealthStatus.HEALTHY
    
    def _on_input_validated(self, event: AgentInputValidated) -> None:
        """Input validation completed"""
        self.state = SessionState.VALIDATING_INPUT
        self.health = SessionHealthStatus.HEALTHY
        
        # Track validation metrics
        self.last_successful_node = "input_validation"
    
    def _on_input_validation_failed(self, event: AgentInputValidationFailed) -> None:
        """Input validation failed"""
        self.state = SessionState.FAILED
        self.health = SessionHealthStatus.FAILED
        self.error_type = "ValidationFailed"
        self.error_message = ", ".join(event.validation_errors)
        self.recoverable = True  # Can retry with different inputs
    
    def _on_node_executed(self, event: AgentNodeExecuted) -> None:
        """Track each LangGraph node execution"""
        self.state = SessionState.EXECUTING
        
        node_record = {
            "node_name": event.node_name,
            "node_sequence": event.node_sequence,
            "duration_ms": event.duration_ms,
            "executed_at": event.executed_at,
            "llm_called": event.llm_called
        }
        self.nodes_executed.append(node_record)
        
        # Track LLM usage
        if event.llm_called and event.llm_tokens_input is not None:
            self.total_llm_calls += 1
            self.total_tokens_input += event.llm_tokens_input
            self.total_tokens_output += event.llm_tokens_output or 0
            self.total_cost_usd += event.llm_cost_usd or 0.0
        
        self.total_duration_ms += event.duration_ms
        self.last_successful_node = event.node_name
        
        # Remove from pending if it was pending
        self.pending_nodes = [n for n in self.pending_nodes 
                             if n["node_name"] != event.node_name]
    
    def _on_tool_called(self, event: AgentToolCalled) -> None:
        """Track tool calls"""
        self.state = SessionState.CALLING_TOOLS
        
        tool_record = {
            "tool_name": event.tool_name,
            "duration_ms": event.tool_duration_ms,
            "called_at": event.called_at
        }
        self.tools_called.append(tool_record)
        self.total_duration_ms += event.tool_duration_ms
        
        # Remove from pending
        self.pending_tool_calls = [t for t in self.pending_tool_calls 
                                  if t["tool_name"] != event.tool_name]
    
    def _on_output_written(self, event: AgentOutputWritten) -> None:
        """Track events written to other aggregates"""
        self.state = SessionState.WRITING_OUTPUT
        
        output_record = {
            "events_count": len(event.events_written),
            "written_at": event.written_at,
            "output_summary": event.output_summary
        }
        self.events_written.append(output_record)
        
        # Track which applications this agent has affected
        for evt in event.events_written:
            if "application_id" in evt.get("payload", {}):
                self.processed_applications.add(evt["payload"]["application_id"])
    
    def _on_session_completed(self, event: AgentSessionCompleted) -> None:
        """Session completed successfully"""
        self.state = SessionState.COMPLETED
        self.health = SessionHealthStatus.COMPLETED
        self.completed_at = event.completed_at
        
        # Update final stats
        self.total_nodes_executed = event.total_nodes_executed
        self.total_llm_calls = event.total_llm_calls
        self.total_tokens_input = event.total_tokens_used
        self.total_cost_usd = event.total_cost_usd
        self.total_duration_ms = event.total_duration_ms
    
    def _on_session_failed(self, event: AgentSessionFailed) -> None:
        """Session failed"""
        self.state = SessionState.FAILED
        self.health = SessionHealthStatus.FAILED
        self.failed_at = event.failed_at
        self.error_type = event.error_type
        self.error_message = event.error_message
        self.recoverable = event.recoverable
        self.last_successful_node = event.last_successful_node
    
    def _on_session_recovered(self, event: AgentSessionRecovered) -> None:
        """Session recovered from previous failure"""
        self.state = SessionState.RECOVERING
        self.health = SessionHealthStatus.RECOVERED
        self.recovered_from_session_id = event.recovered_from_session_id
        self.recovery_point = event.recovery_point
        
        # After recovery, session is healthy again
        self.health = SessionHealthStatus.HEALTHY
        self.state = SessionState.INITIALIZED
    
    # Domain event handlers (for causal chain tracking)
    
    def _on_credit_analysis_completed(self, event: CreditAnalysisCompleted) -> None:
        """Track credit analysis decisions"""
        if hasattr(event, 'application_id'):
            self.processed_applications.add(event.application_id)
    
    def _on_decision_generated(self, event: DecisionGenerated) -> None:
        """Track orchestration decisions"""
        if hasattr(event, 'application_id'):
            self.processed_applications.add(event.application_id)
    
    def _on_fraud_screening_completed(self, event: FraudScreeningCompleted) -> None:
        """Track fraud screening decisions"""
        if hasattr(event, 'application_id'):
            self.processed_applications.add(event.application_id)
    
    def _on_compliance_check_completed(self, event: ComplianceCheckCompleted) -> None:
        """Track compliance decisions"""
        if hasattr(event, 'application_id'):
            self.processed_applications.add(event.application_id)
    
    # Business Rule Validation Methods
    
    def assert_context_loaded(self) -> None:
        """
        Gas Town pattern enforcement.
        
        Rule: No agent decisions without context loaded first.
        Raises: PreconditionFailed if context not loaded
        """
        if not self.context_loaded:
            from ledger.event_store import PreconditionFailed
            raise PreconditionFailed(
                "No active agent session - must call start_agent_session first",
                "start_agent_session"
            )
    
    def assert_model_version_current(self, model_version: str) -> None:
        """
        Model version locking.
        
        Rule: All decisions must use the session's loaded model version.
        Raises: DomainError if version mismatch
        """
        if self.model_version and self.model_version != model_version:
            from ledger.event_store import DomainError
            raise DomainError(
                f"Model version mismatch: session has {self.model_version}, "
                f"command has {model_version}"
            )
    
    def assert_application_processed(self, application_id: str) -> None:
        """
        Causal chain enforcement.
        
        Rule: Agent must have processed this application before claiming to.
        Raises: DomainError if application not processed
        """
        if application_id not in self.processed_applications:
            from ledger.event_store import DomainError
            raise DomainError(
                f"Agent {self.agent_id} has not processed application {application_id}"
            )
    
    def assert_can_execute_node(self, node_name: str) -> None:
        """
        Validate node execution order.
        
        Rule: Cannot execute node if session is in failed state.
        """
        if self.state == SessionState.FAILED:
            from ledger.event_store import DomainError
            raise DomainError(
                f"Cannot execute node {node_name} in failed session state"
            )
    
    # Pending Work Tracking (for Gas Town recovery)
    
    def add_pending_node(self, node_name: str, input_summary: str) -> None:
        """
        Track a node that has started but not completed.
        Used for crash recovery to identify work that needs reconciliation.
        """
        self.pending_nodes.append({
            "node_name": node_name,
            "input_summary": input_summary,
            "added_at": datetime.utcnow().isoformat()
        })
        self.health = SessionHealthStatus.HAS_PENDING_WORK
    
    def add_pending_tool_call(self, tool_name: str, input_summary: str) -> None:
        """
        Track a tool call that has started but not completed.
        """
        self.pending_tool_calls.append({
            "tool_name": tool_name,
            "input_summary": input_summary,
            "added_at": datetime.utcnow().isoformat()
        })
        self.health = SessionHealthStatus.HAS_PENDING_WORK
    
    def has_pending_work(self) -> bool:
        """Check if session has pending work (for crash detection)"""
        return len(self.pending_nodes) > 0 or len(self.pending_tool_calls) > 0
    
    def needs_reconciliation(self) -> bool:
        """
        Determine if session needs reconciliation after crash.
        
        Returns:
            True if there are partial decisions or pending work
        """
        return self.has_pending_work() or self.state == SessionState.FAILED
    
    # Session Summary and State Access
    
    def get_session_summary(self) -> Dict[str, Any]:
        """Get complete session summary for context reconstruction"""
        return {
            "session_id": self.session_id,
            "agent_type": self.agent_type.value if self.agent_type else None,
            "agent_id": self.agent_id,
            "application_id": self.application_id,
            "context_loaded": self.context_loaded,
            "context_source": self.context_source,
            "context_token_count": self.context_token_count,
            "model_version": self.model_version,
            "langgraph_version": self.langgraph_graph_version,
            "state": self.state.value,
            "health": self.health.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "last_heartbeat": self.last_heartbeat.isoformat() if self.last_heartbeat else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "failed_at": self.failed_at.isoformat() if self.failed_at else None,
            "nodes_executed": len(self.nodes_executed),
            "tools_called": len(self.tools_called),
            "events_written": len(self.events_written),
            "processed_applications": list(self.processed_applications),
            "total_llm_calls": self.total_llm_calls,
            "total_tokens_input": self.total_tokens_input,
            "total_tokens_output": self.total_tokens_output,
            "total_cost_usd": round(self.total_cost_usd, 6),
            "total_duration_ms": self.total_duration_ms,
            "has_pending_work": self.has_pending_work(),
            "pending_nodes": self.pending_nodes,
            "pending_tool_calls": self.pending_tool_calls,
            "needs_reconciliation": self.needs_reconciliation(),
            "last_successful_node": self.last_successful_node,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "recoverable": self.recoverable,
            "recovered_from": self.recovered_from_session_id,
            "recovery_point": self.recovery_point,
            "version": self.version
        }
    
    def get_context_for_recovery(self, token_budget: int = 8000) -> Dict[str, Any]:
        """
        Get minimal context for crash recovery (Gas Town pattern).
        
        Args:
            token_budget: Max tokens for recovery context
            
        Returns:
            Dict with essential context for agent to resume
        """
        # Calculate approximate token count for each section
        # Rough estimate: 1 token ≈ 4 characters
        
        context = {
            "session_id": self.session_id,
            "agent_type": self.agent_type.value if self.agent_type else None,
            "application_id": self.application_id,
            "model_version": self.model_version,
            "state": self.state.value,
            "health": self.health.value,
            "needs_reconciliation": self.needs_reconciliation()
        }
        
        # Always include last successful node
        context["last_successful_node"] = self.last_successful_node
        
        # Include pending work if any
        if self.has_pending_work():
            context["pending_work"] = {
                "nodes": self.pending_nodes,
                "tool_calls": self.pending_tool_calls
            }
        
        # Include recent execution history (up to token budget)
        recent_history = []
        token_estimate = len(str(context)) // 4
        
        # Add last 3 nodes executed
        for node in self.nodes_executed[-3:]:
            node_summary = f"Executed {node['node_name']} in {node['duration_ms']}ms"
            recent_history.append(node_summary)
            token_estimate += len(node_summary) // 4
            
            if token_estimate > token_budget:
                recent_history.append("... (additional history truncated)")
                break
        
        context["recent_history"] = recent_history
        
        # Add error context if failed
        if self.state == SessionState.FAILED and self.error_type:
            context["error"] = {
                "type": self.error_type,
                "message": self.error_message,
                "recoverable": self.recoverable
            }
        
        return context
    
    def get_stream_id(self) -> str:
        """Get full stream ID for this session"""
        if not self.agent_type:
            raise ValueError("Agent type not set - cannot construct stream_id")
        return f"agent-{self.agent_type.value}-{self.session_id}"
    
    def get_correlation_id(self) -> str:
        """Get correlation ID for causal chains"""
        return f"session:{self.session_id}"