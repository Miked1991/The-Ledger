# src/integrity/gas_town.py
from typing import List, Dict, Any, Optional
from src.event_store import EventStore
from src.models.events import StoredEvent

class AgentContext:
    """Reconstructed agent context for recovery"""
    def __init__(
        self,
        context_text: str,
        last_event_position: int,
        pending_work: List[Dict[str, Any]],
        session_health_status: str,
        last_completed_action: Optional[Dict[str, Any]] = None
    ):
        self.context_text = context_text
        self.last_event_position = last_event_position
        self.pending_work = pending_work
        self.session_health_status = session_health_status
        self.last_completed_action = last_completed_action
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "context_text": self.context_text,
            "last_event_position": self.last_event_position,
            "pending_work": self.pending_work,
            "session_health_status": self.session_health_status,
            "last_completed_action": self.last_completed_action
        }

async def reconstruct_agent_context(
    store: EventStore,
    agent_id: str,
    session_id: str,
    token_budget: int = 8000
) -> AgentContext:
    """
    Reconstruct agent context from event store for crash recovery.
    Implements Gas Town persistent ledger pattern.
    
    Args:
        store: The event store instance
        agent_id: Agent identifier
        session_id: Session identifier
        token_budget: Token budget for context summarization
        
    Returns:
        AgentContext with reconstructed state for continuation
    """
    stream_id = f"agent-{agent_id}-{session_id}"
    events = await store.load_stream(stream_id)
    
    if not events:
        return AgentContext(
            context_text="No existing session found. Starting fresh.",
            last_event_position=0,
            pending_work=[],
            session_health_status="NEW"
        )
    
    # Analyze events to determine state
    last_completed_action = None
    pending_work = []
    context_loaded = False
    context_text_parts = []
    
    for i, event in enumerate(events):
        if event.event_type == "AgentContextLoaded":
            context_loaded = True
            context_text_parts.append(
                f"Context loaded from {event.payload['context_source']} "
                f"with model {event.payload['model_version']} "
                f"({event.payload['token_count']} tokens)"
            )
        
        elif event.event_type == "AgentActionTaken":
            action = {
                "type": event.payload["action_type"],
                "input_hash": event.payload["input_data_hash"],
                "output": event.payload["output_data"],
                "position": event.stream_position
            }
            
            # Check if this action is incomplete (no corresponding completion)
            # For demo, we consider any action without a following success as pending
            if i == len(events) - 1:
                # Last event - might be incomplete
                if event.payload.get("status") != "completed":
                    pending_work.append(action)
                    context_text_parts.append(
                        f"PENDING: {event.payload['action_type']} action "
                        f"needs reconciliation"
                    )
            else:
                last_completed_action = action
                context_text_parts.append(
                    f"Completed: {event.payload['action_type']} action"
                )
    
    # Check for partial decision (Business Rule #2: NEEDS_RECONCILIATION)
    session_health_status = "HEALTHY"
    if pending_work:
        session_health_status = "NEEDS_RECONCILIATION"
    
    # Summarize events into prose (token-efficient)
    context_summary = " ".join(context_text_parts[-5:])  # Keep last 5 events
    if len(context_summary) > token_budget:
        context_summary = context_summary[:token_budget] + "..."
    
    # Include verbatim: last 3 events, any PENDING or ERROR state events
    last_events = []
    for event in events[-3:]:
        last_events.append({
            "type": event.event_type,
            "payload": event.payload,
            "position": event.stream_position
        })
    
    context_text = (
        f"Session {session_id} recovery:\n"
        f"Context loaded: {context_loaded}\n"
        f"Last position: {events[-1].stream_position if events else 0}\n"
        f"Summary: {context_summary}\n"
        f"Last 3 events: {last_events}\n"
    )
    
    return AgentContext(
        context_text=context_text,
        last_event_position=events[-1].stream_position if events else 0,
        pending_work=pending_work,
        session_health_status=session_health_status,
        last_completed_action=last_completed_action
    )