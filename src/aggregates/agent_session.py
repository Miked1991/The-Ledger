# src/aggregates/agent_session.py
"""
Agent Session Aggregate - Implements Gas Town persistent ledger pattern.
Enforces that agents must load context before making decisions.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
import logging

from src.aggregates.base import Aggregate
from src.event_store import EventStore
from src.models.events import AgentContextLoaded, AgentActionTaken, StoredEvent
from src.models.errors import DomainError, PreconditionFailedError

logger = logging.getLogger(__name__)


class AgentSessionAggregate(Aggregate):
    """
    Agent Session Aggregate implementing Gas Town persistent ledger pattern.
    
    Business Rules:
    - Rule #2: Gas Town - No agent may make a decision without first declaring its context
    - Rule #3: Model version locking - Once context loaded, model version is fixed
    """
    
    def __init__(self, agent_id: str, session_id: str):
        super().__init__()
        self.agent_id = agent_id
        self.session_id = session_id
        self.context_loaded: bool = False
        self.context_source: Optional[str] = None
        self.model_version: Optional[str] = None
        self.token_count: int = 0
        self.context_hash: Optional[str] = None
        self.context_loaded_at: Optional[datetime] = None
        
        self.actions_taken: List[Dict[str, Any]] = []
        self.last_action_type: Optional[str] = None
        self.last_action_at: Optional[datetime] = None
        self.total_actions: int = 0
        
        self.status: str = "ACTIVE"  # ACTIVE, IDLE, ERROR, RECONCILIATION_NEEDED
        self.error_count: int = 0
        self.last_error: Optional[str] = None
    
    @classmethod
    async def load(cls, store: EventStore, agent_id: str, session_id: str) -> "AgentSessionAggregate":
        """Load aggregate from event store"""
        stream_id = f"agent-{agent_id}-{session_id}"
        events = await store.load_stream(stream_id)
        
        agg = cls(agent_id, session_id)
        for event in events:
            agg.apply_event(event)
        
        logger.debug(f"Loaded agent session {agent_id}/{session_id}, context_loaded={agg.context_loaded}")
        return agg
    
    # =========================================================================
    # Event Handlers
    # =========================================================================
    
    def _on_AgentContextLoaded(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle AgentContextLoaded event (Gas Town requirement)"""
        self.context_loaded = True
        self.context_source = payload["context_source"]
        self.model_version = payload["model_version"]
        self.token_count = payload["token_count"]
        self.context_hash = payload["context_hash"]
        self.context_loaded_at = datetime.utcnow()
        self.status = "ACTIVE"
        logger.info(f"Agent {self.agent_id} session {self.session_id} context loaded: {self.context_source}")
    
    def _on_AgentActionTaken(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle AgentActionTaken event"""
        self.actions_taken.append({
            "action_type": payload["action_type"],
            "input_hash": payload["input_data_hash"],
            "reasoning_trace": payload["reasoning_trace"],
            "output_data": payload["output_data"],
            "timestamp": datetime.utcnow().isoformat()
        })
        self.last_action_type = payload["action_type"]
        self.last_action_at = datetime.utcnow()
        self.total_actions += 1
        logger.debug(f"Agent {self.agent_id} action {payload['action_type']} recorded")
    
    def _on_AgentError(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle AgentError event"""
        self.error_count += 1
        self.last_error = payload.get("error_message")
        self.status = "ERROR"
        logger.error(f"Agent {self.agent_id} error: {self.last_error}")
    
    def _on_AgentRecovery(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle AgentRecovery event (Gas Town reconstruction)"""
        self.status = payload.get("new_status", "ACTIVE")
        logger.info(f"Agent {self.agent_id} recovered from crash: {self.status}")
    
    # =========================================================================
    # Business Rule Validation Methods
    # =========================================================================
    
    def assert_context_loaded(self) -> None:
        """
        Rule #2: Gas Town pattern - No agent may make a decision without first declaring its context.
        This is the critical enforcement point for the Gas Town pattern.
        """
        if not self.context_loaded:
            raise PreconditionFailedError(
                f"Agent {self.agent_id} session {self.session_id} must load context before making decisions",
                agent_id=self.agent_id,
                session_id=self.session_id,
                suggested_action="Call start_agent_session with this agent_id and session_id first",
                required_tool="start_agent_session",
                example_call={
                    "tool_name": "start_agent_session",
                    "arguments": {
                        "agent_id": self.agent_id,
                        "session_id": self.session_id,
                        "context_source": "database",
                        "model_version": "v2.0",
                        "token_count": 2048,
                        "context_hash": "hash_here"
                    }
                }
            )
    
    def assert_model_version_current(self, model_version: str) -> None:
        """
        Rule #3: Model version locking - Ensure the model version matches the session's loaded version.
        Prevents analysis churn by ensuring consistent model version across all actions.
        """
        if not self.context_loaded:
            raise PreconditionFailedError(
                f"Cannot check model version: session {self.session_id} has no context loaded",
                suggested_action="Load context first with start_agent_session"
            )
        
        if self.model_version and self.model_version != model_version:
            raise DomainError(
                f"Model version mismatch: session {self.session_id} has {self.model_version}, "
                f"action uses {model_version}",
                session_model_version=self.model_version,
                action_model_version=model_version,
                session_id=self.session_id,
                agent_id=self.agent_id,
                suggested_action="Create new session with correct model version or use existing session's model version"
            )
    
    def assert_action_allowed(self, action_type: str) -> None:
        """Validate that the action is allowed in current session state"""
        self.assert_context_loaded()
        
        if self.status == "ERROR":
            raise PreconditionFailedError(
                f"Agent {self.agent_id} session {self.session_id} is in ERROR state",
                current_status=self.status,
                last_error=self.last_error,
                suggested_action="Call reconstruct_agent_context to recover"
            )
        
        if self.status == "RECONCILIATION_NEEDED":
            raise PreconditionFailedError(
                f"Agent {self.agent_id} session {self.session_id} needs reconciliation",
                current_status=self.status,
                pending_actions=[a for a in self.actions_taken if a.get("status") != "completed"],
                suggested_action="Call reconstruct_agent_context to resume"
            )
    
    def assert_no_duplicate_action(self, action_type: str, input_hash: str) -> None:
        """Prevent duplicate actions with same input"""
        for action in self.actions_taken:
            if action["action_type"] == action_type and action.get("input_hash") == input_hash:
                if self._is_action_completed(action):
                    raise DomainError(
                        f"Duplicate action {action_type} with same input already completed",
                        existing_action=action,
                        suggested_action="Skip or use different input"
                    )
    
    def _is_action_completed(self, action: Dict[str, Any]) -> bool:
        """Check if an action is completed"""
        output = action.get("output_data", {})
        return output.get("status") == "completed" or "result" in output
    
    # =========================================================================
    # Event Creation Methods
    # =========================================================================
    
    def create_context_loaded_event(
        self,
        context_source: str,
        model_version: str,
        token_count: int,
        context_hash: str
    ) -> AgentContextLoaded:
        """Create AgentContextLoaded event (Gas Town pattern)"""
        return AgentContextLoaded(
            agent_id=self.agent_id,
            session_id=self.session_id,
            context_source=context_source,
            token_count=token_count,
            model_version=model_version,
            context_hash=context_hash
        )
    
    def create_action_taken_event(
        self,
        action_type: str,
        input_hash: str,
        reasoning_trace: str,
        output_data: Dict[str, Any]
    ) -> AgentActionTaken:
        """Create AgentActionTaken event"""
        return AgentActionTaken(
            agent_id=self.agent_id,
            session_id=self.session_id,
            action_type=action_type,
            input_data_hash=input_hash,
            reasoning_trace=reasoning_trace,
            output_data=output_data
        )
    
    def create_error_event(
        self,
        error_message: str,
        error_type: str,
        context: Optional[Dict[str, Any]] = None
    ) -> BaseEvent:
        """Create AgentError event"""
        from src.models.events import BaseEvent
        
        class AgentError(BaseEvent):
            event_type: str = "AgentError"
            event_version: int = 1
            agent_id: str
            session_id: str
            error_message: str
            error_type: str
            context: Optional[Dict[str, Any]]
        
        return AgentError(
            agent_id=self.agent_id,
            session_id=self.session_id,
            error_message=error_message,
            error_type=error_type,
            context=context
        )
    
    def create_recovery_event(self, new_status: str = "ACTIVE") -> BaseEvent:
        """Create AgentRecovery event"""
        from src.models.events import BaseEvent
        
        class AgentRecovery(BaseEvent):
            event_type: str = "AgentRecovery"
            event_version: int = 1
            agent_id: str
            session_id: str
            new_status: str
            recovered_at: datetime
        
        return AgentRecovery(
            agent_id=self.agent_id,
            session_id=self.session_id,
            new_status=new_status,
            recovered_at=datetime.utcnow()
        )
    
    # =========================================================================
    # Query Methods
    # =========================================================================
    
    def get_session_summary(self) -> Dict[str, Any]:
        """Get session summary"""
        return {
            "agent_id": self.agent_id,
            "session_id": self.session_id,
            "context_loaded": self.context_loaded,
            "model_version": self.model_version,
            "context_source": self.context_source,
            "token_count": self.token_count,
            "total_actions": self.total_actions,
            "last_action": self.last_action_type,
            "last_action_at": self.last_action_at.isoformat() if self.last_action_at else None,
            "status": self.status,
            "error_count": self.error_count,
            "last_error": self.last_error,
            "version": self.version
        }
    
    def get_action_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent action history"""
        return self.actions_taken[-limit:]
    
    def needs_reconciliation(self) -> bool:
        """Check if session needs reconciliation (Gas Town pattern)"""
        if self.status == "RECONCILIATION_NEEDED":
            return True
        
        # Check for incomplete actions
        for action in self.actions_taken:
            if not self._is_action_completed(action):
                return True
        
        return False